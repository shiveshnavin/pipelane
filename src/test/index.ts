// import the Chai library
import { expect } from 'chai';
import { describe, it } from 'mocha';
import SimplePipeTask from '../impl/SimplePipeTask';
import { OnLog } from '../models/PipeTask';
import PipeLane from '../models/PipeLane';
import { appendFileSync, existsSync, writeFileSync } from 'fs';
import path = require('path');

// describe('PipeLane Test', () => {
//     it('should check equality', () => {

const pipeWork = new PipeLane({
    [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
});
pipeWork.setOnLogSink((str) => {
    str = "\n" + str
    let fileName = path.join(__dirname, "./test.txt")
    if (existsSync(fileName))
        appendFileSync(fileName, str)
    else
        writeFileSync(fileName, str)
})

pipeWork
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
    .then((data) => {
        console.log("DONE")
    })
//     });
// });
