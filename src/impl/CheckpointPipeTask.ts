import PipeTask from "../models/PipeTask";
import type { InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import type PipeWorks from "../models/PipeWorks";


class CheckpointPipeTask extends PipeTask<InputWithPreviousInputs, OutputWithStatus>{

    public static TASK_TYPE_NAME = "CheckpointPipeTask";

    constructor() {
        super(CheckpointPipeTask.TASK_TYPE_NAME, 'default');
    }

    async execute(pipeWorkInstance: PipeWorks, inputs: { last: any[]; }) {
        await pipeWorkInstance._saveCheckpoint()
        return inputs?.last || [{
            status: true,
            time: Date.now()
        }]
    }

    kill(): boolean {
        return true;
    }
}

export default CheckpointPipeTask;