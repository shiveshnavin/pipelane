import PipeTask from "../models/PipeTask";
import type { InputWithPreviousInputs, OutputWithStatus } from "../models/PipeTask";
import type PipeLane from "../models/PipeLane";


class CheckpointPipeTask extends PipeTask<InputWithPreviousInputs, OutputWithStatus>{

    public static TASK_TYPE_NAME = "CheckpointPipeTask";

    private action: string;

    constructor(action?: string) {
        super(CheckpointPipeTask.TASK_TYPE_NAME, action);
        this.action = action || 'create';
    }

    async execute(pipeWorkInstance: PipeLane, inputs: { last: any[]; }) {
        let tmp = pipeWorkInstance.lastTaskOutput
        pipeWorkInstance.lastTaskOutput = inputs.last;
        if (this.action == 'clear')
            await pipeWorkInstance._removeCheckpoint();
        else
            await pipeWorkInstance._saveCheckpoint();
        pipeWorkInstance.lastTaskOutput = tmp;
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