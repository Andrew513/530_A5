
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
    MyDB_RecordPtr llhs = leftTable->getEmptyRecord(), lrhs = leftTable->getEmptyRecord();
    MyDB_RecordPtr rlhs = rightTable->getEmptyRecord(), rrhs = rightTable->getEmptyRecord();
    function<bool ()> leftComparator = buildRecordComparator(llhs, lrhs, equalityCheck.first), rightComparator = buildRecordComparator(rlhs, rrhs, equalityCheck.second), leftComparatorReverse = buildRecordComparator(lrhs, llhs, equalityCheck.first);
    MyDB_RecordIteratorAltPtr myLeftIter = buildItertorOverSortedRuns(64, *leftTable, leftComparator, llhs, lrhs, leftSelectionPredicate);
    MyDB_RecordIteratorAltPtr myRightIter = buildItertorOverSortedRuns(64, *rightTable, rightComparator, rlhs, rrhs, rightSelectionPredicate);

    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema> ();
    for (auto &p : leftTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);
    for(auto &p : rightTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
    combinedRec->buildFrom(llhs, rlhs);

    // func leftEquality = leftInputRec->compileComputation(equalityCheck.first), rightEquality = rightInputRec->compileComputation(equalityCheck.second);
    func larger = combinedRec->compileComputation("> (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func smaller = combinedRec->compileComputation("< (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func equal = combinedRec->compileComputation("== (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

    vector<func> finalComputations;
    for (string s : projections)
        finalComputations.push_back(combinedRec->compileComputation(s));

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    // Must! Otherwise, output will be more than it should
    if(!myLeftIter->advance() || !myRightIter->advance()) {
        return;
    }
    while (true) {
        bool leftEnd = false, rightEnd = false;
        myLeftIter->getCurrent(llhs);
        myRightIter->getCurrent(rlhs);
        if (smaller()->toBool()) {
            if(!myLeftIter->advance()) {
                leftEnd = true;
            }
        } else if (larger()->toBool()) {
            if(!myRightIter->advance()) {
                rightEnd = true;
            }
        } else if (equal()->toBool()) {
            MyDB_PageReaderWriter currentPage(true, *(leftTable->getBufferMgr()));
            vector<MyDB_PageReaderWriter> matchingPages;
            currentPage.append(llhs);
            matchingPages.push_back(currentPage);
            while(true) {
                if(!myLeftIter->advance()) {
                    leftEnd = true;
                    break;
                }
                myLeftIter->getCurrent(lrhs);
                if(!leftComparator() && !leftComparatorReverse()) {
                    if(!currentPage.append(lrhs)) {
                        MyDB_PageReaderWriter newPage(true, *(leftTable->getBufferMgr()));
                        currentPage = newPage;
                        matchingPages.push_back(currentPage);
                        currentPage.append(lrhs);
                    }
                } else {
                    break;
                }
            }
            do {
                if (equal()->toBool()) {
                    MyDB_RecordIteratorAltPtr tempIter = (matchingPages.size() == 1) ? matchingPages[0].getIteratorAlt() 
                    : getIteratorAlt(matchingPages);
                    
                    // Iterate over all matching records
                    for (; tempIter->advance(); ) {
                        tempIter->getCurrent(llhs);
                        if (finalPredicate()->toBool()) {
                            int i = 0;
                            for (auto &f : finalComputations) {
                                outputRec->getAtt(i++)->set(f());
                            }
                            outputRec->recordContentHasChanged();
                            output->append(outputRec);
                        }
                    }
                } else {
                    break;
                }

                // Advance the right iterator and check the condition
                if (!myRightIter->advance()) {
                    rightEnd = true;
                    break;
                }
                myRightIter->getCurrent(rlhs);

            } while (true);

        }
        if(leftEnd || rightEnd) {
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