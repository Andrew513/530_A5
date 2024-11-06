
#ifndef BPLUS_SELECTION_C
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

BPlusSelection ::BPlusSelection(MyDB_BPlusTreeReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                                MyDB_AttValPtr low, MyDB_AttValPtr high,
                                string selectionPredicate, vector<string> projections) : 
                                input(input), output(output), selectionPredicate(selectionPredicate)
                                , low(low), high(high), projections(projections) {}

void BPlusSelection :: run() {
    MyDB_RecordPtr inputRec = input->getEmptyRecord();
    MyDB_RecordPtr outputRec = output->getEmptyRecord();
    func pred = inputRec->compileComputation(selectionPredicate);
    vector<func> computations;
    for (string s : projections) {
        computations.push_back(inputRec->compileComputation(s));
    }
    MyDB_RecordIteratorAltPtr it = input->getRangeIteratorAlt(low, high);
    while(it->advance()) {
        it->getCurrent(inputRec);

        if(pred()->toBool()) {
            for(int i = 0; i < computations.size(); i++) {
                MyDB_AttValPtr thisAtt=outputRec->getAtt(i);
                thisAtt->set(computations[i]());
            }
            outputRec->recordContentHasChanged();
            output->append(outputRec);
        } else {
            continue;
        }
    }
}

#endif
