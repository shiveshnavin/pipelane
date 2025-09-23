import moment = require("moment");
import { type PipeLane } from "./PipeLane";

interface OutputWithStatus {
    status?: boolean
}

interface InputWithPreviousInputs {
    last: OutputWithStatus[],
    additionalInputs: any
}


function OnLog(args: any) {
    let log = '[pipelane] ' + moment(new Date()).format("DD/MM/YYYY hh:mm:ss") + ' ';
    args.forEach(str => {
        if (['number', 'string', 'boolean'].indexOf(typeof str) > -1) {
            log = log.concat(str).concat(' ')
        }
        else {
            log = log.concat(JSON.stringify(str)).concat(' ')
        }
    });
    return log;
}

abstract class PipeTask<I extends InputWithPreviousInputs, O extends OutputWithStatus> {

    public static TASK_VARIANT_NAME: string;
    public static TASK_TYPE_NAME: string;
    public static LOGGING_LEVEL: number = 5;

    public uniqueStepName: string
    public taskVariantName: string;
    public taskTypeName: string;
    public isParallel = false;
    public input: I;
    public outputs: O[];
    public status: boolean;
    public statusMessage: "SUCCESS" | "IN_PROGRESS" | "PAUSED" | "FAILED" | "PARTIAL_SUCCESS" | "SKIPPED";
    public error: string;
    public logs: string[] = [];

    public startTime: Number;
    public endTime: Number;
    public pipeWorkInstance: PipeLane;

    constructor(taskTypeName: string, taskVariantName: string) {
        this.taskTypeName = taskTypeName;
        this.taskVariantName = taskVariantName;
    }

    public onLog = function (...args: any[]) {
        this.logs.push(OnLog(args))
        if (this.pipeWorkInstance.logLevel >= 2) {
            console.log(OnLog(args))
        }
        if (this.pipeWorkInstance?.onLogSink) {
            this.pipeWorkInstance?.onLogSink(OnLog(args))
        }
    }

    public async checkCondition(pipeWorkInstance: PipeLane, inputs: I): Promise<Boolean> {
        return true
    }

    public async _execute(pipeWorkInstance: PipeLane, inputs: I): Promise<O[]> {
        this.init();
        try {
            this.statusMessage = "IN_PROGRESS"
            this.pipeWorkInstance = pipeWorkInstance;
            if (pipeWorkInstance.getOnBeforeExecuteTask()) {
                //@ts-ignore
                inputs = await (pipeWorkInstance.getOnBeforeExecuteTask()(pipeWorkInstance, this, inputs))
            }
            let result = await this.execute(pipeWorkInstance, inputs);
            this.outputs = result;
            this.status = result && result.length > 0;
            this.statusMessage = "SUCCESS"
        } catch (e) {
            this.outputs = [{
                status: false,
                message: e.message
            } as any];
            this.status = false;
            this.statusMessage = "FAILED"
            this.onLog("Error while executing task. ", e.message)
            if (pipeWorkInstance?.logLevel > 0)
                console.log(e)
            if (this.isFastFail(inputs.additionalInputs?.fastFail)) {
                this.onLog("Stopping Pipelane as task is tagged as fastFail=" + inputs.additionalInputs?.fastFail);
                pipeWorkInstance.stop();
            }
            if (this.isFastFail(pipeWorkInstance?.inputs?.fastFail)) {
                this.onLog("Stopping Pipelane as Pipelane is tagged as fastFail=" + pipeWorkInstance?.inputs?.fastFail);
                pipeWorkInstance.stop();
            }
        }
        this.done();
        return this.outputs;
    }

    public isFastFail(fastFail: any) {
        return (fastFail === 1
            || fastFail === '1'
            || fastFail === 'true'
            || fastFail === true)
    }


    public getTaskTypeName(): string {
        return this.taskTypeName;
    }
    public getTaskVariantName(): string {
        return this.taskVariantName;
    }

    /**
     * Called before executing task
     */
    protected init(): any {
        this.startTime = Date.now()
    }

    /**
     * Called after executing task
     */
    protected done(): any {
        this.endTime = Date.now()
    }

    /**
     * Called when task is killed preemptively. Implement your stop task mechanism here.
     */
    abstract kill(): boolean;

    /**
     * Implement your task using this function. It will be called when task is executed
     * @param pipeWorkInstance PipeLane instance
     * @param inputs Inputs include outputs of previous task and any additional inputs
     */
    abstract execute(pipeWorkInstance: PipeLane, inputs: I): Promise<O[]>;

    /**
     * Used to Must provide `cutoffLoadThreshold` in the Variant Config to highlight the load at which * this task will no longer be selected for execution
     * @returns {Number} representing load between 0 - 100 
     */
    public async getLoad(): Promise<number> {
        return 0;
    }

    /**
     * Will be called to wait for task to be unloaded, applications can block this * function till the task is avaialble to pick up work and the return an avaialble task instance
     * @returns `this` if task is ready to pick up work or an instance of * available task, throw an error if this task cant be avaialble now
     */
    public async waitForUnload(): Promise<PipeTask<I, O>> {
        throw new Error('Unimplemented');
    }

    public describe(): PipeTaskDescription | undefined {
        return {
            summary: `A ${this.taskVariantName} task`,
            inputs: {
                last: [],
                additionalInputs: {}
            }
        };
    }
}

type PipeTaskDescription = {
    summary: string
    inputs: {
        last: any[],
        additionalInputs: any
    }
}

export { PipeTask, PipeTaskDescription, OnLog, InputWithPreviousInputs, OutputWithStatus };