import PipeTask, { InputWithPreviousInputs, OnLog, OutputWithStatus } from "./PipeTask";
import * as fs from 'fs';
import * as path from 'path';


interface VariablePipeTask {
    type: string;
    uniqueStepName?: string;
    variantType?: string;
    additionalInputs?: Object;
    getTaskVariant?: (name: String, variantName?: string) => PipeTask<InputWithPreviousInputs, OutputWithStatus>;

    isParallel?: Boolean;
    numberOfShards?: number;
}

class PipeWorks {

    private name: string;
    private workspaceFolder: string;
    private executedTasks: PipeTask<InputWithPreviousInputs, OutputWithStatus>[];
    private currentTaskIdx: number;
    private tasks: VariablePipeTask[];
    private inputs: any;
    private outputs: any;
    private isRunning: boolean;
    private isEnableCheckpoints: boolean;
    private checkpointFolderPath: string;

    private currentExecutionPromises: Promise<any>[];
    private currentExecutionTasks: PipeTask<InputWithPreviousInputs, OutputWithStatus>[];
    private lastTaskOutput: OutputWithStatus[];

    private taskVariantConfig: Map<string, PipeTask<InputWithPreviousInputs, OutputWithStatus>[]>;

    /**
     * 
     * @param taskVariantConfig {"tastType1": MyTaskImplementationClass}
     */
    constructor(taskVariantConfig: Map<string, PipeTask<InputWithPreviousInputs, OutputWithStatus>[]>) {
        this.setTaskVariantsConfig(taskVariantConfig);
        this.workspaceFolder = './pipeworks'
    }

    public setWorkSpaceFolder(path: string) {
        this.workspaceFolder = path;
    }

    public setTaskVariantsConfig(taskVariantConfig: Map<string, PipeTask<InputWithPreviousInputs, OutputWithStatus>[]>) {
        if (!taskVariantConfig) {
            throw Error('Must provide a taskVariantConfig')
        }
        this.taskVariantConfig = taskVariantConfig;
    }

    public enableCheckpoints(pipeName: string, checkpointFolderPath?: string) {
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
    }

    public saveCheckpoint() {
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
        return {
            type: task.type || 'task',
            uniqueStepName: task.uniqueStepName || task.type,
            variantType: task.variantType,
            numberOfShards: task.numberOfShards || 0,
            isParallel: task.isParallel || false,
            getTaskVariant: (type: string, variantType: string) => {
                if (!this.taskVariantConfig.has(type)) {
                    throw Error('Fatal: No task with name' + type + 'exists in taskVariantConfig')
                }
                if (variantType) {
                    let matchingVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus> = undefined;
                    this.taskVariantConfig.forEach((tasks: PipeTask<any, any>[], taskType: string) => {

                        tasks.forEach((taskVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus>) => {
                            if (taskVariant.taskTypeName == type && task.taskVariantName == variantType) {
                                matchingVariant = taskVariant;
                            }
                        })

                    });
                    if (matchingVariant) {
                        return matchingVariant
                    }
                }
                let task: PipeTask<InputWithPreviousInputs, OutputWithStatus> = this.taskVariantConfig.get(type)[0];
                return task;
            }
        }
    }
    private onLog(...args: any[]) {
        console.log(OnLog(args));
    }
    private deserialize(d: Object): PipeWorks {
        return Object.assign(new PipeWorks(this.taskVariantConfig), d);
    }

    private execute() {
        if (this.currentTaskIdx >= this.tasks.length - 1) {
            this.onLog("All tasks completed")
            return
        }
        let tasksToExecute: PipeTask<InputWithPreviousInputs, OutputWithStatus>[] = []
        let curTaskConfig = this.tasks[this.currentTaskIdx++]
        tasksToExecute.push(curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType));
        while (curTaskConfig.isParallel) {
            if (this.currentTaskIdx >= this.tasks.length) {
                break
            }
            curTaskConfig = this.tasks[this.currentTaskIdx]
            if (curTaskConfig.isParallel) {
                tasksToExecute.push(curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType));
                this.currentTaskIdx++;
            }
        }

        let lastTaskOutput: OutputWithStatus[] = this.lastTaskOutput;
        let pw = this;
        this.lastTaskOutput = []
        this.currentExecutionTasks.push(...tasksToExecute);
        this.currentExecutionPromises.push(...tasksToExecute.map((task) => {
            return task._execute(pw, {
                last: lastTaskOutput
            }).then((result: OutputWithStatus) => {
                this.lastTaskOutput.push(result)
            }).catch(e => {
                this.onLog("Error in ", task.taskTypeName, e.message)
                this.lastTaskOutput.push({
                    status: false
                })
            })
        }))

        Promise.all(this.currentExecutionPromises)
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



    /***** ADDING TASKS *****/

    /**
     * Adds a simple task to pipe. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns 
     */
    public pipe(taskConfig: VariablePipeTask): PipeWorks {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }



    public start(inputs: any) {
        this.inputs = inputs;
        this.currentExecutionPromises = [];
        if (this.isEnableCheckpoints) {
            this.loadCheckpoint(this.name);
        }
        this.isRunning = true;
        this.onLog("Started executing pipework", this.name || '')
    }











}

export default PipeWorks;
export { VariablePipeTask };