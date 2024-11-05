
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>
#include <iostream>

using namespace std;

Aggregate :: Aggregate (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
		vector <pair <MyDB_AggType, string>> aggsToComputeIn,
		vector <string> groupingsIn, string selectionPredicateIn) {
            input = inputIn;
            output = outputIn;
            aggsToCompute = aggsToComputeIn;
            groupings = groupingsIn;
            selectionPredicate = selectionPredicateIn;
            numGroups = groupingsIn.size();
        }

void Aggregate :: run () {
    MyDB_SchemaPtr mySchemaAgg = make_shared<MyDB_Schema>(), mySchemaComb = make_shared<MyDB_Schema>();
    for (auto &p : input->getTable()->getSchema()->getAtts()) {
        mySchemaComb->appendAtt(p);
    }
    for (auto &p : output->getTable()->getSchema()->getAtts()) {
        mySchemaAgg->appendAtt(p);
        mySchemaComb->appendAtt(p);
    }

    mySchemaAgg->appendAtt(make_pair("MyDB_cnt", make_shared<MyDB_IntAttType>()));
    mySchemaComb->appendAtt(make_pair("MyDB_cnt", make_shared<MyDB_IntAttType>()));
    
    MyDB_RecordPtr inputRec = input->getEmptyRecord(), aggRec = make_shared<MyDB_Record>(mySchemaAgg);
    vector<func> groupingEqualities;
    for (string s : groupings)
        groupingEqualities.push_back(inputRec->compileComputation(s));

    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaComb);
    combinedRec->buildFrom(inputRec, aggRec);

    int i = 0;
    vector<func> aggComps;
    for (auto &p : aggsToCompute) {
        if (p.first == MyDB_AggType :: sum || p.first == MyDB_AggType :: avg)
            aggComps.push_back(combinedRec->compileComputation("+ (" + p.second + ", [" + output->getTable()->getSchema()->getAtts()[numGroups + i++].first + "])"));
        else
            aggComps.push_back(combinedRec->compileComputation("+ (int[1], [" + output->getTable()->getSchema()->getAtts()[numGroups + i++].first + "])"));
    }
    aggComps.push_back(combinedRec->compileComputation("+ (int[1], [MyDB_cnt])"));
    
    unordered_map<size_t, void*> myHash; // assume each hashVal only matches one group
    func selectPred = inputRec->compileComputation(selectionPredicate);
    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();
    MyDB_PageReaderWriterPtr myAnymPage = make_shared<MyDB_PageReaderWriter>(*(output->getBufferMgr()));

    while (myIter->advance()) {
        myIter->getCurrent(inputRec);
        
        if (!selectPred()->toBool()) {
            continue;
        }

        size_t hashVal = 0;
        for (auto &f : groupingEqualities) {
            hashVal ^= f()->hash();
        }

        if (myHash.count(hashVal) == 0) {
            MyDB_RecordPtr tempRec = make_shared<MyDB_Record>(mySchemaAgg);
            for (int i = 0; i < numGroups; i++) {
                tempRec->getAtt(i)->set(groupingEqualities[i]());
            }

            void *tempPointer = myAnymPage->appendAndReturnLocation(tempRec);
            if (tempPointer == nullptr) {
                myAnymPage = make_shared<MyDB_PageReaderWriter>(*(output->getBufferMgr()));
                tempPointer = myAnymPage->appendAndReturnLocation(tempRec);
            }
            myHash[hashVal] = tempPointer;
        }

        aggRec->fromBinary(myHash[hashVal]);
        
        i = 0;
        for (auto &f : aggComps) {
            aggRec->getAtt(numGroups + i++)->set(f());
        }
        
        aggRec->recordContentHasChanged();
        aggRec->toBinary(myHash[hashVal]);
    }
    
    MyDB_RecordPtr outputRec = output->getEmptyRecord();
    vector<func> finalAggComps;
    i = 0;

    for (auto &p : aggsToCompute) {
        if (p.first == MyDB_AggType :: avg) {
            finalAggComps.push_back(aggRec->compileComputation("/ (" + p.second + ", [MyDB_cnt]"));
        } else {
            finalAggComps.push_back(aggRec->compileComputation("+ (int[0], [" + output->getTable()->getSchema()->getAtts()[numGroups + i].first + "])"));
        }
    }
    
    for (const auto &[v, p] : myHash) {
        aggRec->fromBinary(p);
        for (i = 0; i < groupings.size(); i++) {
            outputRec->getAtt(i)->set(aggRec->getAtt(i));
        }

        for (auto a : finalAggComps) {
            outputRec->getAtt(i++)->set(a());
        }

        outputRec->recordContentHasChanged();
        output->append(outputRec);
    }
}

#endif

