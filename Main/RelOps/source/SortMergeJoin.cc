
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"

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

    func leftEquality = leftInputRec->compileComputation(equalityCheck.first), rightEquality = rightInputRec->compileComputation(equalityCheck.second);
    func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);
    
    vector<func> finalComputations;
    for (string s : projections)
        finalComputations.push_back(combinedRec->compileComputation(s));

    MyDB_RecordPtr outputRec = output->getEmptyRecord();
    while (true) {
        myLeftIter->getCurrent(leftInputRec);
        myRightIter->getCurrent(rightInputRec);

        if (leftEquality()->hash() < rightEquality()->hash() && myLeftIter->advance()) {
            continue;
        } else if (leftEquality()->hash() > rightEquality()->hash() && myRightIter->advance()) {
            continue;
        } else if (leftEquality()->hash() == rightEquality()->hash()) {
            size_t leftHash = leftEquality()->hash();
            myLeftIter->memorizeState();
            while (leftHash == rightEquality()->hash()) {
                myRightIter->getCurrent(rightInputRec);
                myLeftIter->setState();
                while (leftEquality()->hash() == rightEquality()->hash()) {
                    myLeftIter->getCurrent(leftInputRec);
                    if (finalPredicate()->toBool()) {
                        int i = 0;
                        for (auto &f : finalComputations)
                            outputRec->getAtt(i++)->set(f());
                        outputRec->recordContentHasChanged();
                        output->append(outputRec);
                    }

                    if (!(myLeftIter->advance()))
                        break;
                }

                if (!(myRightIter->advance()))
                    break;
            }
        } else {
            break;
        }
    }

    // unordered_map <size_t, vector<void *>> myHash;
    // while (myLeftIter->advance()) {
    //     myLeftIter->getCurrent(leftInputRec);
    //     size_t hashVal = leftEquality()->hash();
    //     myHash[hashVal].push_back(myLeftIter->getCurrentPointer());
    // }

    // while (myRightIter->advance()) {
    //     myRightIter->getCurrent(rightInputRec);
    //     size_t hashVal = rightEquality()->hash();

    //     if (myHash.count(hashVal) == 0)
    //         continue;
    
    //     vector<void *> &potentialMatches = myHash[hashVal];
    //     for (auto &v : potentialMatches) {
    //         leftInputRec->fromBinary(v);

    //         if (finalPredicate()->toBool()) {
    //             int i = 0;
    //             for (auto &f : finalComputations) {
    //                 outputRec->getAtt(i++)->set(f());
    //             }
    //             outputRec->recordContentHasChanged();
    //             output->append(outputRec);
    //         }

    //        
    //     }
    // }
}

#endif
