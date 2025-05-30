import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import { PipeLane } from '../models/PipeLane';

describe('PipeLane Test', () => {

    it('should execute conditionally', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        }, 'test');

        //@ts-ignore
        pipeWork.setOnCheckCondition((pl, task, inputs) => {
            return !inputs.additionalInputs?.skip
        })

        let skippedCount = 0
        pipeWork.logLevel = 0
        let data = await pipeWork
            .setListener((pl, ev, task, payload) => {
                if (ev == 'SKIPPED') {
                    skippedCount++
                }
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step2',
                additionalInputs: {
                    skip: true
                }
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step3',
                additionalInputs: {
                    skip: true
                }
            })
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step4'
            }).start()
        expect(data[0].count).to.equal(2)
        expect(skippedCount).to.equal(2)
        expect(data[0].status).to.equal(true);
    })

    it('should should execute sequentially', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        }, 'test');
        pipeWork.logLevel = 0
        let notRecievedEvents = {
            'START': 1,
            'NEW_TASK': 4,
            'TASK_FINISHED': 4,
            'COMPLETE': 1
        }
        let data = await pipeWork
            .setListener((pl, ev, task, payload) => {
                notRecievedEvents[ev]--
            })
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

        Object.keys(notRecievedEvents)
            .forEach(missingEv => {
                expect(notRecievedEvents[missingEv]).to.equal(0)
            })
        expect(data[0].count).to.equal(4)
        expect(data[0].status).to.equal(true);
    })


    it('should complete successfully', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        }, 'test');
        let notRecievedEvents = {
            'START': 1,
            'NEW_TASK': 9,
            'TASK_FINISHED': 9,
            'COMPLETE': 1
        }
        let onBeforeExecCount = 0
        let data = await pipeWork
            .setOnBeforeExecuteTask(async (pl, task, input) => {
                onBeforeExecCount++
                return input
            })
            .setListener((pl, ev, task, payload) => {
                notRecievedEvents[ev]--
            })
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

        Object.keys(notRecievedEvents)
            .forEach(missingEv => {
                expect(missingEv + '=' + notRecievedEvents[missingEv]).to.equal(missingEv + '=' + 0)
            })

        expect(onBeforeExecCount).to.equal(10)
        expect(data[0].count).to.equal(7)
        expect(data[0].status).to.equal(true);
    });
});
