#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include <vector>
#include <iostream>

SortMergeJoin::SortMergeJoin(MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
                             MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
                             vector<string> projectionsIn,
                             pair<string, string> equalityCheckIn, string leftSelectionPredicateIn,
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

void SortMergeJoin::run() {
    // Sort left table on equalityCheck.first and sort right table on equalityCheck.second
    MyDB_RecordPtr llhs = leftTable->getEmptyRecord(), lrhs = leftTable->getEmptyRecord();
    MyDB_RecordPtr rlhs = rightTable->getEmptyRecord(), rrhs = rightTable->getEmptyRecord();
    function<bool()> leftComparator = buildRecordComparator(llhs, lrhs, equalityCheck.first);
    function<bool()> rightComparator = buildRecordComparator(rlhs, rrhs, equalityCheck.second);
    MyDB_RecordIteratorAltPtr myLeftIter = buildItertorOverSortedRuns(64, *leftTable, leftComparator, llhs, lrhs, leftSelectionPredicate);
    MyDB_RecordIteratorAltPtr myRightIter = buildItertorOverSortedRuns(64, *rightTable, rightComparator, rlhs, rrhs, rightSelectionPredicate);

    MyDB_RecordPtr leftInputRec = leftTable->getEmptyRecord(), rightInputRec = rightTable->getEmptyRecord();
    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
    for (auto &p : leftTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);
    for (auto &p : rightTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
    combinedRec->buildFrom(leftInputRec, rightInputRec);

    // Build comparator for left record and right record
    func larger = combinedRec->compileComputation("> (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func smaller = combinedRec->compileComputation("< (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func equal = combinedRec->compileComputation("== (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

    vector<func> finalComputations;
    for (string s : projections)
        finalComputations.push_back(combinedRec->compileComputation(s));

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    while (true) {
        // cout << "Fetching current left and right records..." << endl;
        myLeftIter->getCurrent(leftInputRec);
        myRightIter->getCurrent(rightInputRec);

        // cout << "Comparing left and right records." << endl;
        if (smaller()->toBool()) {
            // cout << "Left record is smaller, advancing left iterator." << endl;
            if (!myLeftIter->advance()) {
                // cout << "Left iterator reached the end." << endl;
                break;
            }
            continue;
        } else if (larger()->toBool()) {
            // cout << "Right record is smaller, advancing right iterator." << endl;
            if (!myRightIter->advance()) {
                // cout << "Right iterator reached the end." << endl;
                break;
            }
            continue;
        } else if (equal()->toBool()) {
            // cout << "Records are equal, collecting matching left records..." << endl;
            vector<void *> records;

            myLeftIter->getCurrent(leftInputRec);
            while (equal()->toBool()) {
                // cout << "Adding left record to vector." << endl;
                records.push_back(myLeftIter->getCurrentPointer());

                if (!myLeftIter->advance()) {
                    // cout << "Left iterator exhausted while collecting matches." << endl;
                    return;
                }
                myLeftIter->getCurrent(leftInputRec);
            }

            // cout << "Processing right records for matches." << endl;
            myRightIter->getCurrent(rightInputRec);
            while (equal()->toBool()) {
                for (void *r : records) {
                    leftInputRec->fromBinary(r);
                    if (finalPredicate()->toBool()) {
                        // cout << "Final predicate matched, appending output record." << endl;
                        int i = 0;
                        for (auto &f : finalComputations) {
                            outputRec->getAtt(i++)->set(f());
                        }
                        outputRec->recordContentHasChanged();
                        output->append(outputRec);
                    }
                }

                if (!myRightIter->advance()) {
                    // cout << "Right iterator exhausted while processing matches." << endl;
                    return;
                }
                myRightIter->getCurrent(rightInputRec);
            }
        } else {
            // cout << "No match found, breaking loop." << endl;
            break;
        }
    }
}

#endif
