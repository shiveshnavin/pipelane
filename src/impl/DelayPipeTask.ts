import { PipeTask, InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import { type PipeLane } from "../models/PipeLane";


class DelayPipeTask extends PipeTask<InputWithPreviousInputs, OutputWithStatus> {

    public static TASK_TYPE_NAME = "DelayPipeTask";

    private sleepForMs = 1000;

    constructor(sleepForMs: number) {
        super(DelayPipeTask.TASK_TYPE_NAME, 'default');
        this.sleepForMs = sleepForMs;
    }

    async execute(pipeWorkInstance: PipeLane, inputs: { last: any[]; }) {
        let count = inputs?.last ? inputs?.last[0]?.count || 0 : 0
        this.onLog("Sleeping for", this.sleepForMs / 1000, 'seconds')
        await new Promise(resolve => setTimeout(resolve, this.sleepForMs));
        return inputs?.last || [{
            status: true,
            time: Date.now()
        }]
    }

    kill(): boolean {
        this.onLog("DelayPipeTask Kill Requested")
        return true;
    }
}

export default DelayPipeTask;