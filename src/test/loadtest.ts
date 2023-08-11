import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import PipeLane from '../models/PipeLane';
import { appendFileSync, existsSync, writeFileSync } from 'fs';
import path = require('path');

PipeLane.LOGGING_LEVEL = 0
describe('PipeLane Load Test', () => {
    it('should fail due to overload of specified variant', async () => {

        const pipeWork = new PipeLane({
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1', 100), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
        });
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
        let data = await pipeWork
            .pipe({
                type: SimplePipeTask.TASK_TYPE_NAME,
                uniqueStepName: 'Step1'
            }).start()
        console.log("DONE")
        expect(data[0].status).to.equal(true);

    });
});
