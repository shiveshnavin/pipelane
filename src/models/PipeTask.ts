import moment = require("moment");
import PipeWorks from "./PipeWorks";

interface OutputWithStatus {
    status?: boolean
}

interface InputWithPreviousInputs {
    last: OutputWithStatus[]
}


function OnLog(args: any) {
    let log = moment(new Date()).format("DD/MM/YYYY hh:mm:ss") + ' ';
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


    private taskVariantName: string;
    private taskTypeName: string;
    public isParallel = false;
    public input: I;
    public outputs: O[];
    public status: boolean;
    public error: string;
    public logs: string[] = [];

    private startTime: Number;
    private endTime: Number;

    constructor(taskTypeName: string, taskVariantName: string) {
        this.taskTypeName = taskTypeName;
        this.taskVariantName = taskVariantName;
    }

    protected onLog = function (...args: any[]) {
        this.logs.push(OnLog(args))
        if (PipeWorks.LOGGING_LEVEL >= 2) {
            console.log(OnLog(args))
        }
    }


    public init(): any {
        this.startTime = Date.now()
    }

    public done(): any {
        this.endTime = Date.now()
    }

    public async _execute(pipeWorkInstance: PipeWorks, inputs: I): Promise<O[]> {
        this.init();
        try {
            let result = await this.execute(pipeWorkInstance, inputs, this.onLog);
            this.outputs = result;
            this.status = result && result.length > 0;
        } catch (e) {
            this.outputs = undefined;
            this.status = false;
            this.onLog("Error while executing task. ", e.message)
            console.log(e)
        }
        this.done();
        return this.outputs;
    }

    public getTaskTypeName(): string {
        return this.taskTypeName;
    }
    public getTaskVariantName(): string {
        return this.taskVariantName;
    }

    abstract kill(): boolean;
    abstract execute(pipeWorkInstance: PipeWorks, inputs: I, onLog: Function): Promise<O[]>;

}

export { OnLog, InputWithPreviousInputs, OutputWithStatus };
export default PipeTask;