// import the Chai library
import { expect } from 'chai';
import { describe, it } from 'mocha';
import DelayPipeTask from '../impl/DelayPipeTask';
import SimplePipeTask from '../impl/SimplePipeTask';
import { OnLog } from '../models/PipeTask';
import PipeWorks from '../models/PipeWorks';

describe('PipeWorks Test', () => {
    it('should check equality', () => {

        const pipeWork = new PipeWorks({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1')],
            [DelayPipeTask.TASK_TYPE_NAME]: [new DelayPipeTask(3000)]
        }).pipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step1'
        }).pipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step2'
        }).pipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step3'
        }).parallelPipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step4'
        }).parallelPipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step5'
        }).shardedPipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step6',
            numberOfShards: 2
        }).pipe({
            type: DelayPipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step7'
        }).pipe({
            type: SimplePipeTask.TASK_TYPE_NAME,
            uniqueStepName: 'Step8'
        }).start();
    });
});
