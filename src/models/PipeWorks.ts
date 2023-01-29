import PipeTask, { InputWithPreviousInputs, OnLog, OutputWithStatus } from "./PipeTask";
import * as fs from 'fs';
import * as path from 'path';
import moment = require("moment");
import * as lodash from 'lodash';


function splitArray<T>(arr: T[], pieces: number): T[][] {
    let result: T[][] = [];
    let chunkSize = Math.ceil(arr.length / pieces);

    for (let i = 0, j = arr.length; i < j; i += chunkSize) {
        result.push(arr.slice(i, i + chunkSize));
    }

    return result;
}

interface VariablePipeTask {
    type: string;
    uniqueStepName?: string;
    variantType?: string;
    additionalInputs?: Object;
    getTaskVariant?: (name: String, variantName?: string) => PipeTask<InputWithPreviousInputs, OutputWithStatus>;

    isParallel?: Boolean;
    numberOfShards?: number;
}


interface TaskVariantConfig {
    [key: string]: PipeTask<InputWithPreviousInputs, OutputWithStatus>[]
}

class PipeWorks {

    public static LOGGING_LEVEL = 5;

    private name: string;
    private workspaceFolder: string;
    private executedTasks: PipeTask<InputWithPreviousInputs, OutputWithStatus>[] = [];
    private currentTaskIdx: number = 0;
    private tasks: VariablePipeTask[] = [];
    private inputs: any;
    private outputs: any;
    private isRunning: boolean;
    private isEnableCheckpoints: boolean;
    private checkpointFolderPath: string;

    private currentExecutionPromises: Promise<any>[] = [];
    private currentExecutionTasks: {
        task: PipeTask<InputWithPreviousInputs, OutputWithStatus>,
        inputs: OutputWithStatus[]
    }[] = [];
    private lastTaskOutput: OutputWithStatus[];

    private taskVariantConfig: TaskVariantConfig;

    /**
     * 
     * @param {TaskVariantConfig} taskVariantConfig Specify the variant implementations for each task
     */
    constructor(taskVariantConfig: TaskVariantConfig) {
        this.setTaskVariantsConfig(taskVariantConfig);
        this.workspaceFolder = './pipeworks'
    }

    public setWorkSpaceFolder(path: string): PipeWorks {
        this.workspaceFolder = path;
        return this;
    }

    public setTaskVariantsConfig(taskVariantConfig: TaskVariantConfig): PipeWorks {
        if (!taskVariantConfig) {
            throw Error('Must provide a taskVariantConfig')
        }
        this.taskVariantConfig = taskVariantConfig;
        return this;
    }

    public enableCheckpoints(pipeName: string, checkpointFolderPath?: string): PipeWorks {
        if (!pipeName) {
            this.onLog("Undefined checkpoint name. Checkpoints not enabled!")
            return
        }
        if (!checkpointFolderPath) {
            checkpointFolderPath = './pipeworks/'
        }
        this.name = pipeName;
        this.checkpointFolderPath = checkpointFolderPath;
        this.isEnableCheckpoints = true;
        return this;
    }

    private saveCheckpoint() {
        let chFile = path.join(this.checkpointFolderPath, `./${this.name}.json`);
        fs.writeFileSync(chFile, JSON.stringify(this, undefined, 2))
        this.onLog("Checkpoint saved to", chFile)
    }

    public loadCheckpoint(pipeName: string) {
        let chFile = path.join(this.checkpointFolderPath, `./${pipeName}.json`);
        if (!fs.existsSync(chFile)) {
            this.onLog("No checkpoint found in path.", chFile)
            return
        }
        else {
            this.onLog("Loading checkpoint from", chFile)
        }
        let checkPointBlob = fs.readFileSync(chFile).toString();
        let obj: PipeWorks = this.deserialize(JSON.parse(checkPointBlob));
        this.executedTasks = obj.executedTasks;
        this.inputs = obj.inputs;
        this.tasks = obj.tasks;
        this.isRunning = false;
        this.name = pipeName;
        this.currentTaskIdx = obj.currentTaskIdx;
        this.currentExecutionPromises = [];
    }


    private defaultVariablePipeTaskParams(task: VariablePipeTask): VariablePipeTask {
        if (task.uniqueStepName && this.tasks.map(ts => ts.uniqueStepName).indexOf(task.uniqueStepName) > -1) {
            throw new Error("A step with same uniqueStepName '" + task.uniqueStepName + "' already exists")
        }
        return {
            type: task.type || 'task',
            uniqueStepName: task.uniqueStepName || task.variantType || task.type,
            variantType: task.variantType,
            numberOfShards: task.numberOfShards || 0,
            isParallel: task.isParallel || false,
            getTaskVariant: (type: string, variantType: string) => {
                if (!this.taskVariantConfig.hasOwnProperty(type)) {
                    throw Error('Fatal: No task with name ' + type + 'exists in taskVariantConfig')
                }
                if (variantType) {
                    let matchingVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus> = undefined;
                    Object.keys(this.taskVariantConfig).forEach((key: string) => {
                        let tasks = this.taskVariantConfig[key];
                        tasks.forEach((taskVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus>) => {
                            if (taskVariant.getTaskTypeName() == type && task.getTaskVariantName() == variantType) {
                                matchingVariant = taskVariant;
                            }
                        })

                    });
                    if (matchingVariant) {
                        let clone = lodash.cloneDeep(matchingVariant);
                        return clone
                    }
                }
                let task: PipeTask<InputWithPreviousInputs, OutputWithStatus> = this.taskVariantConfig[type][0];
                if (!task) {
                    throw Error("No task defined in taskVariantConfig of type " + type)
                }
                let clone = lodash.cloneDeep(task);
                return clone
            }
        }
    }

    private onLog = function (...args: any[]) {
        if (PipeWorks.LOGGING_LEVEL >= 2) {
            console.log(OnLog(args))
        }
    }

    private deserialize(d: Object): PipeWorks {
        return Object.assign(new PipeWorks(this.taskVariantConfig), d);
    }

    private execute(): Promise<any> {
        if (this.currentTaskIdx >= this.tasks.length) {
            this.onLog("All tasks completed")
            return
        }
        let lastTaskOutputs: OutputWithStatus[] = this.lastTaskOutput;
        let tasksToExecute: {
            task: PipeTask<InputWithPreviousInputs, OutputWithStatus>,
            inputs: OutputWithStatus[]
        }[] = []
        let curTaskConfig = this.tasks[this.currentTaskIdx++]

        if (lastTaskOutputs && curTaskConfig.numberOfShards > 0 && curTaskConfig.numberOfShards <= lastTaskOutputs.length) {
            let inputShards = splitArray(lastTaskOutputs, curTaskConfig.numberOfShards)
            inputShards.forEach(shardInput => {
                tasksToExecute.push({
                    task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: shardInput
                });
            })
        }
        else {
            tasksToExecute.push({
                task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                inputs: lastTaskOutputs
            });
        }

        if (PipeWorks.LOGGING_LEVEL > 3) {
            this.onLog('Executing step', curTaskConfig.uniqueStepName)
        }

        while (curTaskConfig.isParallel) {
            if (this.currentTaskIdx >= this.tasks.length) {
                break
            }
            curTaskConfig = this.tasks[this.currentTaskIdx]
            if (curTaskConfig.isParallel) {
                tasksToExecute.push({
                    task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: lastTaskOutputs
                });
                if (PipeWorks.LOGGING_LEVEL > 3) {
                    this.onLog('Executing step', curTaskConfig.uniqueStepName)
                }
                this.currentTaskIdx++;
            }
        }

        let pw = this;
        this.lastTaskOutput = []
        this.currentExecutionTasks.push(...tasksToExecute);
        this.currentExecutionPromises.push(...tasksToExecute.map((taskExecution) => {
            return taskExecution.task._execute(pw, {
                last: taskExecution.inputs
            }).then((result: OutputWithStatus[]) => {
                this.lastTaskOutput.push(...result)
            }).catch(e => {
                this.onLog("Error in ", taskExecution.task.getTaskTypeName(), e.message)
                this.lastTaskOutput.push({
                    status: false
                })
            })
        }))

        return Promise.all(this.currentExecutionPromises)
            .then(results => {
                this.execute();
            })
            .catch(e => {
                this.onLog("Error executing tasks", e.message)
                this.execute();
            })

    }

    public pause() {

    }




    /**
     * Adds a simple sequential task to pipe. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeWorks
     */
    public pipe(taskConfig: VariablePipeTask): PipeWorks {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }


    /**
     * Adds a parallel task to pipe. Each parallel task gets complete output from the last task from the There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeWorks
     */
    public parallelPipe(taskConfig: VariablePipeTask): PipeWorks {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        config.isParallel = true;
        this.tasks.push(config);
        return this;
    }

    /**
     * Adds a parallel task to pipe. Similar to `parallelPipe` with the key difference that the output from previous task are divided into `numberOfShards` groups and fed to the parallel tasks. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeWorks
     */
    public shardedPipe(taskConfig: VariablePipeTask): PipeWorks {
        if (!taskConfig.numberOfShards) {
            throw new Error("Must specify numberOfShards")
        }
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }



    /**
     * 
     * @param inputs Inputs for the PipeWork
     * @returns A promise which will resolve when all the tasks are completed
     */
    public start(inputs?: any): Promise<any> {
        this.inputs = inputs;
        this.currentExecutionPromises = [];
        if (this.isEnableCheckpoints) {
            this.loadCheckpoint(this.name);
        }
        this.isRunning = true;
        this.onLog("Started executing pipework", this.name || '')

        return this.execute();
    }











}

export default PipeWorks;
export { VariablePipeTask, TaskVariantConfig };