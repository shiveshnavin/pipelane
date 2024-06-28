import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import { PipeLane } from '../models/PipeLane';
import { appendFileSync, existsSync, writeFileSync } from 'fs';
import path = require('path');
import { PipeTask } from '../models/PipeTask';

describe('PipeLane Load Test', () => {
    it('should fail due to overload of specified variant', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 100), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                variantType: 'simplevar1',
                uniqueStepName: 'Step1'
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(false);

    });

    it('should pass due to overload of specified variant but hasnt reached threshold', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 18), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                variantType: 'simplevar1',
                uniqueStepName: 'Step1',
                cutoffLoadThreshold: 19
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(true);

    });


    it('should fail due to overload of specified variant', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 19), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                variantType: 'simplevar1',
                uniqueStepName: 'Step1',
                cutoffLoadThreshold: 19
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(false);

    });


    it('should fail due to overload of all variants', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 67), new SimplePipeTask('simplevar2', 56), new SimplePipeTask('simplevar3', 67)]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1',
                cutoffLoadThreshold: 56
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(false);

    });

    it('should pass due to overload of all variants but one', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 67), new SimplePipeTask('simplevar2', 10), new SimplePipeTask('simplevar3', 67)]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1',
                cutoffLoadThreshold: 56
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(true);

    });

    it('should fail due to overload of all variants', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 100), new SimplePipeTask('simplevar2', 100), new SimplePipeTask('simplevar3', 100)]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(false);

    });

    it('should pass due to overload of all but one variant', async () => {
        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 100), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
        pipeWork.logLevel = 0
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(true);

    });

    it('should wait for task to be unloaded', async () => {

        class LoadedTask extends SimplePipeTask {
            static TASK_TYPE_NAME = 'loaded'
            public async getLoad(): Promise<number> {
                return 100;
            }

            async execute(pipeWorkInstance: PipeLane, inputs: { last: any[]; }): Promise<any> {
                return [{
                    msg: 'ok'
                }]
            }

            public async waitForUnload(): Promise<PipeTask<any, any>> {
                await new Promise(resolve => setTimeout(resolve, 5000));
                if (this.getTaskVariantName().indexOf("will-resolve") > -1) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    return this
                } else {
                    throw new Error('Will not be avaialble')
                }
            }
        }

        const pipeWork = new PipeLane({
            ['loaded']: [
                new LoadedTask('will-not-resolve-0'),
                new LoadedTask('will-not-resolve-1'),
                new LoadedTask('will-not-resolve-2'),
                new LoadedTask('will-not-resolve-3'),
                new LoadedTask('will-resolve'),
                new LoadedTask('will-not-resolve-4'),
            ]
        });

        let data = await pipeWork
            .pipe({
                type: LoadedTask.TASK_TYPE_NAME,
                uniqueStepName: 'loaded'
            }).start()
        expect(data[0].msg).to.equal('ok')

        console.log("DONE")
    });
});
