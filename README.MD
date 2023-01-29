# PipeWorks
A library to orchestrate a set of tasks where each task could have variants. Support for resume using checkpoints.

## Installation
```
npm install pipeworks
```

## Basic Usage
```js
// Implement your task by implementating interface `PipeTask` as a class.
// Register your task and its variants in variant config
const variantConfig = {
            [SimplePipeTask.TASK_TYPE_NAME]: [new SimplePipeTask('simplevar1'), new SimplePipeTask('simplevar2'), new SimplePipeTask('simplevar3')]
};

const pipeWork = new PipeWorks(variantConfig).enableCheckpoints('test')
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
            }).start();
```


## Functions

### enableCheckpoints
Enable checkpoint support. Pass a name of the pipe as a parameter.

### checkpoint
Create checkpoint with current state of pipe. When the start() is called again, the tasks resume from where left.

### clearCheckpoint
Clear checkpoints if exists

### pipe
Add a sequential task to pipeline.
```js
  .pipe({
         type: SimplePipeTask.TASK_TYPE_NAME // Mandatory
         variantType: 'simplevar2' // optional, if absent the first task of the fiven type from the variants will be picked
       })
```

### parallelPipe
Add a parallel task to pipeline.

### shardedPipe
Similar to `parallelPipe` with key difference that, the input to this task is split into `numberOfShards` groups and each group is fed to a task. Each of these shards are executed parallely and the output is collected.
```js
  .shardedPipe({
         type: SimplePipeTask.TASK_TYPE_NAME, // Mandatory
         numberOfShards: 2 // Mandatory
       })
```
