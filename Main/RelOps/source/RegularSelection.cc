
#ifndef REG_SELECTION_C
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection ::RegularSelection(MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                                    string selectionPredicate, vector<string> projections) : input(input), output(output),
                                                              selectionPredicate(selectionPredicate), projections(projections) {}

void RegularSelection ::run()
{
    MyDB_RecordPtr inputRec = input->getEmptyRecord();
    MyDB_RecordPtr outputRec = output->getEmptyRecord();
    func pred = inputRec->compileComputation(selectionPredicate);
    vector<func> computations;
    for (string s : projections) {
        computations.push_back(inputRec->compileComputation(s));
    }

    MyDB_RecordIteratorPtr it = input->getIterator(inputRec);
    while (it->hasNext()) {
        it->getNext();
        if (pred()->toBool()) {
            for (int i = 0; i < computations.size(); i++) {
                MyDB_AttValPtr thisAtt = outputRec->getAtt(i);
                thisAtt->set(computations[i]());
            }
            outputRec->recordContentHasChanged();
            output->append(outputRec);
        }
    }
}

#endif
