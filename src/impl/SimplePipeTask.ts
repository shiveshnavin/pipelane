import { PipeTask, InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import { PipeLane } from "../models/PipeLane";


class SimplePipeTask extends PipeTask<InputWithPreviousInputs, { count: number } & OutputWithStatus> {

    public static TASK_TYPE_NAME = "SimplePipeTask";
    private currentLoad: number;
    constructor(taskVariantName: string, currentLoad: number = 0) {
        super(SimplePipeTask.TASK_TYPE_NAME, taskVariantName);
        this.currentLoad = currentLoad
    }

    async execute(pipeWorkInstance: PipeLane, inputs: { last: any[]; }) {
        let count = inputs?.last ? inputs?.last[0]?.count || 0 : 0
        this.onLog("Hello from", this.getTaskTypeName(), this.getTaskVariantName(), 'count=', count)
        await new Promise(resolve => setTimeout(resolve, 100));
        return [{
            status: true,
            count: count + 1,
            time: Date.now() + Math.random() * 100
        }]
    }

    kill(): boolean {
        return true;
    }

    public async getLoad(): Promise<number> {
        return this.currentLoad
    }
}

export default SimplePipeTask;