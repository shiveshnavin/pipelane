// import the Chai library
import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import { OnLog } from '../models/PipeTask';
import PipeLane from '../models/PipeLane';

describe('PipeLane Test', () => {
    it('should check equality', () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        }).enableCheckpoints('test')
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step2',
                variantType: 'simplevar3'
            })
            .sleep(1000)
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step3'
            })
            .checkpoint()
            .parallelPipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step4',
                variantType: 'simplevar2'
            }).parallelPipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step5'
            }).shardedPipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step6',
                numberOfShards: 2
            }).pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step7'
            })
            .clearCheckpoint()
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step8'
            }).start()
            .then((data) => {
                console.log("DONE")
            })
    });
});
