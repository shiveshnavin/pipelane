import PipeTask from "../models/PipeTask";
import type { InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import type PipeWorks from "../models/PipeWorks";


class CheckpointPipeTask extends PipeTask<InputWithPreviousInputs, OutputWithStatus>{

    public static TASK_TYPE_NAME = "CheckpointPipeTask";

    private action: string;

    constructor(action?: string) {
        super(CheckpointPipeTask.TASK_TYPE_NAME, action);
        this.action = action || 'create';
    }

    async execute(pipeWorkInstance: PipeWorks, inputs: { last: any[]; }) {
        if (this.action == 'clear')
            await pipeWorkInstance._removeCheckpoint()
        else
            await pipeWorkInstance._saveCheckpoint()
        return inputs?.last || [{
            status: true,
            time: Date.now()
        }]
    }

    kill(): boolean {
        this.onLog("CheckpointPipeTask Kill Requested")
        return true;
    }
}

export default CheckpointPipeTask;