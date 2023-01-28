// import the Chai library
import { expect } from 'chai';
import { describe, it } from 'mocha';
import { OnLog } from '../models/PipeTask';
import PipeWorks from '../models/PipeWorks';

describe('PipeWorks Test', () => {
    it('should check equality', () => {
        const pipe = new PipeWorks(undefined)
        console.log(OnLog('Error in test', { a: 2 }))
    });
});
