import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import PipeLane from '../models/PipeLane';

PipeLane.LOGGING_LEVEL = 0
describe('PipeLane Test', () => {

    it('should should execute sequentially', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step2'
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step3'
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step4'
            }).start()

        expect(data[0].count).to.equal(4)
        expect(data[0].status).to.equal(true);
    })


    it('should complete successfully', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });

        let data = await pipeWork
            // .enableCheckpoints('test')
            //     .clearCheckpoint()

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
            // .checkpoint()
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
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step8'
            }).start()

        console.log("DONE")
        expect(data[0].count).to.equal(7)
        expect(data[0].status).to.equal(true);
    });
});
