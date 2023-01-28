import moment = require("moment");
import PipeWorks from "./PipeWorks";

type OutputWithStatus = {
    status: boolean
}

type InputWithPreviousInputs = {
    last: OutputWithStatus[]
}


function OnLog(...args: any[]) {
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

    public static TASK_VARIANT_NAME: String;
    public static TASK_TYPE_NAME: String;


    public taskVariantName: String;
    public taskTypeName: String;
    public isParallel = false;
    public input: I;
    public output: O;
    public status: Boolean;
    public error: String;
    public logs: String[] = [];

    private startTime: Number;
    private endTime: Number;
    private onLog = function (...args: any[]) {
        this.logs.push(OnLog(args))
    }


    public init(): any {
        this.startTime = Date.now()
    }

    public done(): any {
        this.endTime = Date.now()
    }

    public async _execute(pipeWorkInstance: PipeWorks, inputs: I): Promise<OutputWithStatus> {
        this.init();
        try {
            let result = await this.execute(pipeWorkInstance, inputs, this.onLog);
            this.output = result;
            this.status = result.status;
        } catch (e) {
            this.output = undefined;
            this.status = false;
            this.onLog("Error while executing task. ", e.message)
        }
        this.done();
        return this.output;
    }

    abstract kill(): Boolean;
    abstract execute(pipeWorkInstance: PipeWorks, inputs: I, onLog: Function): Promise<O>;

}

export { OnLog, InputWithPreviousInputs, OutputWithStatus };
export default PipeTask;