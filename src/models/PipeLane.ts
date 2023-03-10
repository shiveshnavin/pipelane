import PipeTask, { InputWithPreviousInputs, OnLog, OutputWithStatus } from "./PipeTask";
import * as fs from 'fs';
import * as path from 'path';
import moment = require("moment");
import * as lodash from 'lodash';
import DelayPipeTask from "../impl/DelayPipeTask";
import CheckpointPipeTask from "../impl/CheckpointPipeTask";


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
    itemsPerShard?: number;
}


interface TaskVariantConfig {
    [key: string]: PipeTask<InputWithPreviousInputs, OutputWithStatus>[]
}

class PipeLane {

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
    private onLogSink: Function;

    private currentExecutionPromises: Promise<any>[] = [];
    private currentExecutionTasks: {
        task: PipeTask<InputWithPreviousInputs, OutputWithStatus>,
        inputs: InputWithPreviousInputs
    }[] = [];
    public lastTaskOutput: OutputWithStatus[];

    private taskVariantConfig: TaskVariantConfig;

    /**
     * 
     * @param {TaskVariantConfig} taskVariantConfig Specify the variant implementations for each task
     */
    constructor(taskVariantConfig: TaskVariantConfig) {
        this.setTaskVariantsConfig(taskVariantConfig);
        this.workspaceFolder = './pipelane'
    }

    public setOnLogSink(onLogSink) {
        this.onLogSink = onLogSink;
    }

    public getInputs(): any {
        if (!this.inputs)
            this.inputs = {}
        return this.inputs;
    }

    public setWorkSpaceFolder(path: string): PipeLane {
        this.workspaceFolder = path;
        return this;
    }

    public setTaskVariantsConfig(taskVariantConfig: TaskVariantConfig): PipeLane {
        if (!taskVariantConfig) {
            throw Error('Must provide a taskVariantConfig')
        }
        this.taskVariantConfig = Object.assign({
            [DelayPipeTask.TASK_TYPE_NAME]: [new DelayPipeTask(1000)],
            [CheckpointPipeTask.TASK_TYPE_NAME]: [new CheckpointPipeTask('create'), new CheckpointPipeTask('clear')]
        }, taskVariantConfig);
        return this;
    }

    public enableCheckpoints(pipeName: string, checkpointFolderPath?: string): PipeLane {
        if (!pipeName) {
            this.onLog("Undefined checkpoint name. Checkpoints not enabled!")
            return
        }
        if (!checkpointFolderPath) {
            checkpointFolderPath = './pipelane/'
        }
        this.name = pipeName;
        this.checkpointFolderPath = checkpointFolderPath;
        this.isEnableCheckpoints = true;

        if (!fs.existsSync(this.workspaceFolder)) {
            fs.mkdirSync(this.workspaceFolder)
        }
        if (!fs.existsSync(this.checkpointFolderPath)) {
            fs.mkdirSync(this.checkpointFolderPath)
        }
        return this;
    }

    public async _saveCheckpoint() {
        try {
            let chFile = path.join(this.checkpointFolderPath, `./checkpoint_${this.name}.json`);
            let json = JSON.stringify(this, (key, value) => {
                if (key === 'pipeWorkInstance') {
                    return undefined;
                }
                return value;
            })
            fs.writeFileSync(chFile, JSON.stringify(JSON.parse(json), undefined, 2));
            this.onLog("Checkpoint saved to", chFile);
        } catch (e) {
            console.log("Tolerable Error saving checkpoint", e)
        }
    }

    public async _removeCheckpoint() {
        let chFile = path.join(this.checkpointFolderPath, `./checkpoint_${this.name}.json`);
        if (fs.existsSync(chFile))
            fs.unlinkSync(chFile)
        this.onLog("Checkpoint removed", chFile)
    }

    private _loadCheckpoint(pipeName: string) {
        let chFile = path.join(this.checkpointFolderPath, `./checkpoint_${pipeName}.json`);
        if (!fs.existsSync(chFile)) {
            this.onLog("No checkpoint found in path.", chFile)
            return
        }
        else {
            this.onLog("Loading checkpoint from", chFile)
        }
        let checkPointBlob = fs.readFileSync(chFile).toString();
        let obj: PipeLane = this.deserialize(JSON.parse(checkPointBlob));
        this.executedTasks = obj.executedTasks;
        this.inputs = obj.inputs;
        this.lastTaskOutput = obj.lastTaskOutput
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
            uniqueStepName: task.uniqueStepName,
            variantType: task.variantType,
            numberOfShards: task.numberOfShards || 0,
            itemsPerShard: task.itemsPerShard || 0,
            isParallel: task.isParallel || false,
            additionalInputs: task.additionalInputs,
            getTaskVariant: (type: string, variantType: string) => {
                if (!this.taskVariantConfig.hasOwnProperty(type)) {
                    throw Error('Fatal: No task with name ' + type + 'exists in taskVariantConfig')
                }
                if (variantType) {
                    let matchingVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus> = undefined;
                    Object.keys(this.taskVariantConfig).forEach((key: string) => {
                        let tasks = this.taskVariantConfig[key];
                        tasks.forEach((taskVariant: PipeTask<InputWithPreviousInputs, OutputWithStatus>) => {
                            if (taskVariant.getTaskTypeName() == type && taskVariant.getTaskVariantName() == variantType) {
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

    public onLog = function (...args: any[]) {
        if (PipeLane.LOGGING_LEVEL >= 2) {
            console.log(OnLog(args))
        }
        if (this.onLogSink) {
            this.onLogSink(OnLog(args))
        }
    }

    private deserialize(d: Object): PipeLane {
        return Object.assign(new PipeLane(this.taskVariantConfig), d);
    }

    private execute(): Promise<any> {
        this.isRunning = true;
        if (this.currentTaskIdx >= this.tasks.length) {
            this.onLog("All tasks completed")
            this.isRunning = false;
            return
        }
        let lastTaskOutputs: OutputWithStatus[] = this.lastTaskOutput;
        let tasksToExecute: {
            task: PipeTask<InputWithPreviousInputs, OutputWithStatus>,
            inputs: InputWithPreviousInputs
        }[] = []
        let curTaskConfig = this.tasks[this.currentTaskIdx++]

        if (lastTaskOutputs && curTaskConfig.itemsPerShard > 0) {
            curTaskConfig.numberOfShards = Math.max(1, lastTaskOutputs.length / (curTaskConfig.itemsPerShard))
            let inputShards = splitArray(lastTaskOutputs, curTaskConfig.numberOfShards)
            inputShards.forEach(shardInput => {
                tasksToExecute.push({
                    task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: {
                        last: shardInput,
                        additionalInputs: curTaskConfig.additionalInputs
                    }
                });
            })
        }
        else if (lastTaskOutputs && curTaskConfig.numberOfShards > 0 && curTaskConfig.numberOfShards <= lastTaskOutputs.length) {
            let inputShards = splitArray(lastTaskOutputs, curTaskConfig.numberOfShards)
            inputShards.forEach(shardInput => {
                tasksToExecute.push({
                    task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: {
                        last: shardInput,
                        additionalInputs: curTaskConfig.additionalInputs
                    }
                });
            })
        }
        else {
            tasksToExecute.push({
                task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                inputs: {
                    last: lastTaskOutputs,
                    additionalInputs: curTaskConfig.additionalInputs
                }
            });
        }

        if (PipeLane.LOGGING_LEVEL > 3) {
            this.onLog('Executing step', curTaskConfig.uniqueStepName || curTaskConfig.variantType || curTaskConfig.type)
        }

        while (curTaskConfig.isParallel) {
            if (this.currentTaskIdx >= this.tasks.length) {
                break
            }
            curTaskConfig = this.tasks[this.currentTaskIdx]
            if (curTaskConfig.isParallel) {
                tasksToExecute.push({
                    task: curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: {
                        last: lastTaskOutputs,
                        additionalInputs: curTaskConfig.additionalInputs
                    }
                });
                if (PipeLane.LOGGING_LEVEL > 3) {
                    this.onLog('Executing step', curTaskConfig.uniqueStepName)
                }
                this.currentTaskIdx++;
            }
        }

        let pw = this;
        this.lastTaskOutput = []
        this.currentExecutionTasks = []
        this.currentExecutionPromises = []

        this.currentExecutionTasks.push(...tasksToExecute);
        this.currentExecutionPromises.push(...tasksToExecute.map((taskExecution) => {
            return taskExecution.task._execute(pw, taskExecution.inputs).then((result: OutputWithStatus[]) => {
                this.lastTaskOutput.push(...result)
            }).catch(e => {
                this.onLog("Error in ", taskExecution.task.getTaskTypeName(), e.message)
                this.lastTaskOutput.push({
                    status: false
                })
            })
        }))

        return new Promise((resolve, reject) => {
            Promise.all(this.currentExecutionPromises)
                .then(async (results) => {
                    if (this.isRunning)
                        await this.execute();
                    resolve(this.lastTaskOutput)
                })
                .catch(async (e) => {
                    console.log(e)
                    this.onLog("Error executing tasks", e.message)
                    if (this.isRunning)
                        await this.execute();
                    resolve(this.lastTaskOutput)
                })
        })

    }


    /**
     * Pause current execution 
     */
    public async stop() {
        this.isRunning = false;
        if (this.enableCheckpoints)
            this._saveCheckpoint()
        this.currentExecutionTasks.forEach(ts => {
            ts.task.kill()
        })

        await Promise.all(this.currentExecutionPromises)
    }


    /**
     * Adds a simple sequential task to pipe. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeLane
     */
    public pipe(taskConfig: VariablePipeTask): PipeLane {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }


    /**
     * Adds a parallel task to pipe. Each parallel task gets complete output from the last task from the There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeLane
     */
    public parallelPipe(taskConfig: VariablePipeTask): PipeLane {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        config.isParallel = true;
        this.tasks.push(config);
        return this;
    }

    /**
     * Adds a parallel task to pipe. Similar to `parallelPipe` with the key difference that the output from previous task are divided into `numberOfShards` groups and fed to the parallel tasks. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeLane
     */
    public shardedPipe(taskConfig: VariablePipeTask): PipeLane {
        if (!taskConfig.numberOfShards && !taskConfig.itemsPerShard) {
            throw new Error("Must specify numberOfShards or itemsPerShard")
        }
        if (taskConfig.numberOfShards && taskConfig.itemsPerShard) {
            throw new Error("Cannot specify both numberOfShards and itemsPerShard")
        }
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }


    /**
     * Creates a checkpoint with the current pipeline state
     * @returns {PipeLane}
     */
    public checkpoint(): PipeLane {
        let config = this.defaultVariablePipeTaskParams({
            type: CheckpointPipeTask.TASK_TYPE_NAME,
            variantType: 'create'
        })
        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }


    /**
     * Removes a checkpoint with the current pipeline state
     * @returns {PipeLane}
     */
    public clearCheckpoint(): PipeLane {
        this._removeCheckpoint();
        return this;
    }

    /**
    * Adds a delay in execution of next task
    * @param sleepMs Delay in Miliseconds 
    * @returns PipeLane
    */
    public sleep(sleepMs: number): PipeLane {
        let config = this.defaultVariablePipeTaskParams({
            type: DelayPipeTask.TASK_TYPE_NAME
        })

        config.getTaskVariant(config.type);
        this.tasks.push(config);
        return this;
    }


    /**
     * 
     * @param inputs Inputs for the PipeWork
     * @returns A promise which will resolve when all the tasks are completed
     */
    public async start(inputs?: any): Promise<any> {
        this.inputs = inputs;
        this.currentExecutionPromises = [];
        if (this.isEnableCheckpoints) {
            this._loadCheckpoint(this.name);
        }
        this.isRunning = true;
        this.onLog("Started executing pipework", this.name || '')

        await this.execute();
        return this.lastTaskOutput;
    }











}

export default PipeLane;
export { VariablePipeTask, TaskVariantConfig };