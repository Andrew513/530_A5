
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include <vector>

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
		MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn, 
		vector <string> projectionsIn,
		pair <string, string> equalityCheckIn, string leftSelectionPredicateIn,
		string rightSelectionPredicateIn) {
            leftTable = leftInput;
            rightTable = rightInput;
            output = outputIn;
            finalSelectionPredicate = finalSelectionPredicateIn;
            projections = projectionsIn;
            equalityCheck = equalityCheckIn;
            leftSelectionPredicate = leftSelectionPredicateIn;
            rightSelectionPredicate = rightSelectionPredicateIn;
        }

void SortMergeJoin :: run () {
    // sort left table on equalityCheck.first and sort right table on equalityCheck.second
    MyDB_RecordPtr llhs = leftTable->getEmptyRecord(), lrhs = leftTable->getEmptyRecord();
    MyDB_RecordPtr rlhs = rightTable->getEmptyRecord(), rrhs = rightTable->getEmptyRecord();
    function<bool ()> leftComparator = buildRecordComparator(llhs, lrhs, equalityCheck.first), rightComparator = buildRecordComparator(rlhs, rrhs, equalityCheck.second);
    MyDB_RecordIteratorAltPtr myLeftIter = buildItertorOverSortedRuns(64, *leftTable, leftComparator, llhs, lrhs, leftSelectionPredicate);
    MyDB_RecordIteratorAltPtr myRightIter = buildItertorOverSortedRuns(64, *rightTable, rightComparator, rlhs, rrhs, rightSelectionPredicate);

    MyDB_RecordPtr leftInputRec = leftTable->getEmptyRecord(), rightInputRec = rightTable->getEmptyRecord();
    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema> ();
    for (auto &p : leftTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);
    for(auto &p : rightTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
    combinedRec->buildFrom(leftInputRec, rightInputRec);

    // build comparator for left record and right record
    func larger = combinedRec->compileComputation("> (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func smaller = combinedRec->compileComputation("< (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func equal = combinedRec->compileComputation("== (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

    vector<func> finalComputations;
    for (string s : projections)
        finalComputations.push_back(combinedRec->compileComputation(s));

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    myLeftIter->advance();
    myRightIter->advance();
    while (true) {
        myLeftIter->getCurrent(leftInputRec);
        myRightIter->getCurrent(rightInputRec);
        if (smaller()->toBool() && myLeftIter->advance()) {
            // if left record is smaller, left advance
            continue;
        } else if (larger()->toBool() && myRightIter->advance()) {
            // if right record is smaller, right advance
            continue;
        } else if (equal()->toBool()) {
            // put all left record havings the same equality check att as the current right record into records
            // ex: left table [(1, x), (1, y), (1, z), (2, x), (2, y), (3, x), (3, y), (3, z), ...], right record = (2, x)
            //     records will be [(2, x), (2, y)]
            vector<void *> records;
            bool leftEnd = false;
            myLeftIter->getCurrent(leftInputRec);
            while (equal()->toBool()) {
                records.push_back(myLeftIter->getCurrentPointer());
                
                if (!(myLeftIter->advance())) {
                    leftEnd = true;
                    break;
                }
                myLeftIter->getCurrent(leftInputRec);
            }

            if (records.empty()) 
                continue;
            // check if each left record in records match current right record, if true, combine and append
            // do above step for all right records having the same equalityCheck att as the current right record
            // ex: right Table = [(1, a), (2, a), (2, b), (2, x), (2, y), (3, x), (3, z), ...], cur right record = (2, x)
            //     records = [(2, x), (2, y)]
            //     will check (2, a), (2, b), (2, x), (2, y) on records
            cout << "record size: " << records.size() << " ";
            leftInputRec->fromBinary(records[0]);
            myRightIter->getCurrent(rightInputRec);
            int cnt = 0;
            while (equal()->toBool()) {
                cnt += 1;
                for (void* r : records) {
                    leftInputRec->fromBinary(r);
                    if (finalPredicate()->toBool()) {
                        int i = 0;
                        for (auto &f : finalComputations)
                            outputRec->getAtt(i++)->set(f());
                        outputRec->recordContentHasChanged();
                        output->append(outputRec);
                    }
                }

                if (!(myRightIter->advance())) {
                    break;
                }
                myRightIter->getCurrent(rightInputRec);
            }
            cout << cnt << ", " << records.size() << endl;
            if (leftEnd)
                break;
        } else {
            break;
        }
    }
}

#endif
