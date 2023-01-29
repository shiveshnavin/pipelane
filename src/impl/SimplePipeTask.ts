import PipeTask, { InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import PipeWorks from "../models/PipeWorks";


class SimplePipeTask extends PipeTask<InputWithPreviousInputs, { count: number } & OutputWithStatus>{

    public static TASK_TYPE_NAME = "SimplePipeTask";

    constructor(taskVariantName: string) {
        super(SimplePipeTask.TASK_TYPE_NAME, taskVariantName);
    }

    async execute(pipeWorkInstance: PipeWorks, inputs: { last: any[]; }, onLog: Function) {
        let count = inputs?.last ? inputs?.last[0]?.count || 0 : 0
        this.onLog("Hello from", this.getTaskTypeName(), this.getTaskVariantName(), 'count=', count)
        await new Promise(resolve => setTimeout(resolve, 2000));
        return [{
            status: true,
            count: count + 1,
            time: Date.now() + Math.random() * 100
        }]
    }

    kill(): boolean {
        return true;
    }
}

export default SimplePipeTask;