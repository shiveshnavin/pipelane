import { PipeTask, InputWithPreviousInputs, OnLog, OutputWithStatus } from "./PipeTask";
import * as fs from 'fs';
import * as path from 'path';
import moment = require("moment");
import * as lodash from 'lodash';
import DelayPipeTask from "../impl/DelayPipeTask";
import CheckpointPipeTask from "../impl/CheckpointPipeTask";


function waitForFirstPromise(promises): any {
    return new Promise((resolve, reject) => {
        let rejectedCount = 0;

        promises.forEach((promise) => {
            promise
                .then((result) => {
                    resolve(result);
                })
                .catch((error) => {
                    rejectedCount++;
                    if (rejectedCount === promises.length) {
                        resolve(undefined);
                    }
                });
        });
    });
}


function splitArray<T>(arr: T[], pieces: number): T[][] {
    let result: T[][] = [];
    let chunkSize = Math.ceil(arr.length / pieces);

    for (let i = 0, j = arr.length; i < j; i += chunkSize) {
        result.push(arr.slice(i, i + chunkSize));
    }

    return result;
}

async function forEachAsync(arr: any, cb: Function) {
    for (let index = 0; index < arr.length; index++) {
        const element = arr[index];
        await cb(element, index)

    }
}

export interface VariablePipeTask {
    type: string;
    uniqueStepName?: string;
    variantType?: string;
    additionalInputs?: Object;
    getTaskVariant?: (name: String, variantName?: string) => Promise<PipeTask<InputWithPreviousInputs, OutputWithStatus>>;

    isParallel?: Boolean;
    numberOfShards?: number;
    itemsPerShard?: number;
    cutoffLoadThreshold?: number;
}


export type PipelaneEvent = 'START' | 'COMPLETE' | 'NEW_TASK' | 'TASK_FINISHED' | 'KILLED'

export interface TaskVariantConfig {
    [key: string]: PipeTask<InputWithPreviousInputs, OutputWithStatus>[]
}

export type OnBeforeExecute = (
    pipeWorkInstance: PipeLane,
    task: PipeTask<any, any>,
    inputs: InputWithPreviousInputs) => Promise<InputWithPreviousInputs>

export type OnCheckCondition = (
    pipeWorkInstance: PipeLane,
    task: PipeTask<any, any>,
    inputs: InputWithPreviousInputs) => Promise<Boolean>

export type PipeLaneListener = (
    pipelaneInstance: PipeLane,
    event: PipelaneEvent,
    task: VariablePipeTask | undefined,
    payload: any | undefined) => void


export class PipeLane {

    public logLevel: 0 | 1 | 2 | 3 | 4 | 5 = 5;

    public name: string;
    public workspaceFolder: string;
    private executedTasks: PipeTask<InputWithPreviousInputs, OutputWithStatus>[] = [];
    public currentTaskIdx: number = 0;
    private tasks: VariablePipeTask[] = [];
    public inputs: any;
    private outputs: any;
    public isRunning: boolean;
    private schedule: String
    private active: Boolean
    private isEnableCheckpoints: boolean;
    private checkpointFolderPath: string;
    private onLogSink: Function;
    private listener: PipeLaneListener;
    private onBeforeExecute: OnBeforeExecute
    private onCheckCondition: OnCheckCondition

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
            if (!this.checkpointFolderPath) {
                this.onLog(`Skipping saving checkpoint as checkpointFolderPath is not defined. did you call enableCheckpoints(<checkpoint-name>)`)
                return
            }
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
            if (this.logLevel > 0)
                console.log("Tolerable Error saving checkpoint", e)
        }
    }


    public getOnBeforeExecuteTask(): OnBeforeExecute {
        return this.onBeforeExecute;
    }
    public setOnBeforeExecuteTask(onBeforeExecute: OnBeforeExecute): PipeLane {
        this.onBeforeExecute = onBeforeExecute;
        return this
    }

    public getOnCheckCondition(): OnCheckCondition {
        return this.onCheckCondition;
    }
    public setOnCheckCondition(onCheckCondition: OnCheckCondition): PipeLane {
        this.onCheckCondition = onCheckCondition;
        return this
    }

    private getListener(): PipeLaneListener {
        if (!this.listener) {
            return (a, b, c, d) => { }
        }
        return this.listener;
    }
    public setListener(listener: PipeLaneListener): PipeLane {
        this.listener = listener;
        return this
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
        let fnGetDefaultTaskVariant = async (type: string, variantType: string) => {
            if (!this.taskVariantConfig.hasOwnProperty(type)) {
                throw Error('Fatal: No task with name ' + type + 'exists in taskVariantConfig')
            }
            if (!task.cutoffLoadThreshold)
                task.cutoffLoadThreshold = 100
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
                    let clone: PipeTask<InputWithPreviousInputs, OutputWithStatus> = lodash.cloneDeep(matchingVariant);
                    let currentLoad = await clone.getLoad()
                    if ((currentLoad) >= task.cutoffLoadThreshold) {
                        this.onLog(`Found a task defined in taskVariantConfig of type "${type}" and variantType "${variantType}" but the Current load = ${currentLoad} already exceeded threshold ${task.cutoffLoadThreshold}.`)
                        this.stop()
                        throw Error(`Found a task defined in taskVariantConfig of type "${type}" and variantType ${variantType} but the Current load = ${currentLoad} already exceeded threshold ${task.cutoffLoadThreshold}.`)
                    }
                    return clone
                }
                else {
                    throw Error(`No task defined in taskVariantConfig of type "${type}" and variantType ${variantType}`)
                }
            }

            let selectedTask: PipeTask<InputWithPreviousInputs, OutputWithStatus>;
            await forEachAsync(this.taskVariantConfig[type], async (curTask: PipeTask<InputWithPreviousInputs, OutputWithStatus>) => {
                if (!selectedTask)
                    if ((await curTask.getLoad()) < (task.cutoffLoadThreshold)) {
                        selectedTask = curTask;
                    }
            })
            if (!selectedTask) {
                if (this.taskVariantConfig[type]?.length == 0) {
                    throw Error(`No task defined in taskVariantConfig of type ${type}`)
                }
                let allWaitForUnloads = this.taskVariantConfig[type].map(task => {
                    return task.waitForUnload()
                })
                this.onLog(`All tasks of type ${type} have load already exceeding threshold of ${task.cutoffLoadThreshold}. Waiting for any of ${allWaitForUnloads.length} tasks to become unloaded`)
                selectedTask = await waitForFirstPromise(allWaitForUnloads)
                if (!selectedTask) {
                    throw Error(`Unrecoverable error: All tasks of type ${type} have load already exceeding threshold of ${task.cutoffLoadThreshold} and are not gonna be avaialable anytime soon !`)
                } else {
                    this.onLog(`${selectedTask.getTaskVariantName()} just got unloaded. Continuing...`)
                }
            }
            let clone = lodash.cloneDeep(selectedTask);
            return clone
        }
        return {
            type: task.type || 'task',
            uniqueStepName: task.uniqueStepName,
            variantType: task.variantType,
            numberOfShards: task.numberOfShards || 0,
            itemsPerShard: task.itemsPerShard || 0,
            isParallel: task.isParallel || false,
            additionalInputs: task.additionalInputs,
            cutoffLoadThreshold: task.cutoffLoadThreshold || 100,
            getTaskVariant: task.getTaskVariant || fnGetDefaultTaskVariant
        }
    }

    public onLog = function (...args: any[]) {
        if (this.logLevel >= 2) {
            console.log(OnLog(args))
        }
        if (this.onLogSink) {
            this.onLogSink(OnLog(args))
        }
    }

    private deserialize(d: Object): PipeLane {
        return Object.assign(new PipeLane(this.taskVariantConfig), d);
    }

    private async execute(): Promise<any> {
        this.isRunning = true;
        if (this.currentTaskIdx >= this.tasks.length) {
            this.onLog("All tasks completed")
            this.isRunning = false;
            this.outputs = this.lastTaskOutput
            this.getListener()(this, 'COMPLETE', undefined, this.outputs)
            return
        }
        let lastTaskOutputs: OutputWithStatus[] = this.lastTaskOutput;
        let tasksToExecute: {
            taskConfig: VariablePipeTask,
            task: PipeTask<InputWithPreviousInputs, OutputWithStatus>,
            inputs: InputWithPreviousInputs
        }[] = []

        let curTaskConfig = this.tasks[this.currentTaskIdx++]
        this.getListener()(this, 'NEW_TASK', curTaskConfig, undefined)
        try {

            if (lastTaskOutputs && curTaskConfig.itemsPerShard > 0) {
                curTaskConfig.numberOfShards = Math.max(1, lastTaskOutputs.length / (curTaskConfig.itemsPerShard))
                let inputShards = splitArray(lastTaskOutputs, curTaskConfig.numberOfShards)
                await forEachAsync(inputShards, async (shardInput) => {
                    tasksToExecute.push({
                        taskConfig: curTaskConfig,
                        task: await curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                        inputs: {
                            last: shardInput,
                            additionalInputs: curTaskConfig.additionalInputs
                        }
                    });
                })
            }
            else if (lastTaskOutputs && curTaskConfig.numberOfShards > 0 && curTaskConfig.numberOfShards <= lastTaskOutputs.length) {
                let inputShards = splitArray(lastTaskOutputs, curTaskConfig.numberOfShards)
                await forEachAsync(inputShards, async (shardInput) => {
                    tasksToExecute.push({
                        taskConfig: curTaskConfig,
                        task: await curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                        inputs: {
                            last: shardInput,
                            additionalInputs: curTaskConfig.additionalInputs
                        }
                    });
                })
            }
            else {
                tasksToExecute.push({
                    taskConfig: curTaskConfig,
                    task: (await curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType)),
                    inputs: {
                        last: lastTaskOutputs,
                        additionalInputs: curTaskConfig.additionalInputs
                    }
                });
            }
        } catch (e) {

            if (this.logLevel > 2) {
                console.log(e)
            }
            if (!this.lastTaskOutput) {
                this.lastTaskOutput = []
            }
            this.lastTaskOutput.push({
                status: false
            })
            this.onLog("Fatal error while retrieving task variant", e.message)
            this.stop()
            return
        }

        if (this.logLevel > 3) {
            this.onLog('Executing step', curTaskConfig.uniqueStepName || curTaskConfig.variantType || curTaskConfig.type)
        }
        while (curTaskConfig.isParallel) {
            if (this.currentTaskIdx >= this.tasks.length) {
                break
            }
            curTaskConfig = this.tasks[this.currentTaskIdx]
            if (curTaskConfig.isParallel) {
                this.getListener()(this, 'NEW_TASK', curTaskConfig, undefined)
                tasksToExecute.push({
                    taskConfig: curTaskConfig,
                    task: await curTaskConfig.getTaskVariant(curTaskConfig.type, curTaskConfig.variantType),
                    inputs: {
                        last: lastTaskOutputs,
                        additionalInputs: curTaskConfig.additionalInputs
                    }
                });
                if (this.logLevel > 3) {
                    this.onLog('Executing step', curTaskConfig.uniqueStepName)
                }
                this.currentTaskIdx++
            }

        }

        let pw = this;
        this.lastTaskOutput = []
        this.currentExecutionTasks = []
        this.currentExecutionPromises = []

        this.currentExecutionTasks.push(...tasksToExecute);
        let taskTypePromisesSplit: any = {}

        this.currentExecutionPromises.push(...tasksToExecute.map((taskExecution) => {

            let promise = taskExecution.task._execute(pw, taskExecution.inputs).then((result: OutputWithStatus[]) => {
                this.lastTaskOutput.push(...result)
                return result
            }).catch(e => {
                this.onLog("Error in ", taskExecution.task.getTaskTypeName(), e.message)
                this.lastTaskOutput.push({
                    status: false
                })
                return [{
                    status: false
                }]
            }).finally(() => {
                this.executedTasks?.push(taskExecution.task)
            })
            let key = taskExecution.taskConfig.uniqueStepName || taskExecution.taskConfig.variantType
            let existingTaskTypePromisesMap = taskTypePromisesSplit[key]
            if (!existingTaskTypePromisesMap) {
                existingTaskTypePromisesMap = {
                    config: taskExecution.taskConfig,
                    promises: []
                }
                taskTypePromisesSplit[key] = existingTaskTypePromisesMap
            }
            existingTaskTypePromisesMap.promises.push(promise)
            return promise
        }))

        Object.keys(taskTypePromisesSplit).forEach(key => {
            Promise.all(taskTypePromisesSplit[key].promises).then(function (results: any[]) {
                if (results instanceof Array && results[0] instanceof Array)
                    results = results.flat()
                pw.getListener()(pw, 'TASK_FINISHED', taskTypePromisesSplit[key].config, results)
            })
        })

        return new Promise((resolve, reject) => {
            Promise.all(this.currentExecutionPromises)
                .then(async (results) => {
                    if (this.isRunning)
                        await this.execute();
                    resolve(this.lastTaskOutput)
                })
                .catch(async (e) => {
                    if (this.logLevel > 0)
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
    public async reset() {
        this.currentTaskIdx = 0
        this.outputs = undefined
        this.isRunning = false
        await this.stop()
        this.currentExecutionTasks = []
    }



    /**
     * Pause current execution 
     */
    public async stop() {
        this.isRunning = false;
        if (this.isEnableCheckpoints)
            this._saveCheckpoint()
        this.currentExecutionTasks.forEach(ts => {
            ts.task.kill()
        })

        await Promise.all(this.currentExecutionPromises)
        this.getListener()(this, 'KILLED', undefined, this.outputs)
    }


    /**
     * Adds a simple sequential task to pipe. There can be multiple variants of a task which is defined in taskConfig
     * @param taskConfig 
     * @returns PipeLane
     */
    public pipe(taskConfig: VariablePipeTask): PipeLane {
        let config = this.defaultVariablePipeTaskParams(taskConfig)

        config.getTaskVariant(config.type, config.variantType);
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

        config.getTaskVariant(config.type, config.variantType);
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

        config.getTaskVariant(config.type, config.variantType);
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
        config.getTaskVariant(config.type, config.variantType);
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

        config.getTaskVariant(config.type, config.variantType);
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
        this.getListener()(this, 'START', undefined, undefined)
        await this.execute();
        return this.lastTaskOutput;
    }

}
