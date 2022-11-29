/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id: tupleannexstep.cpp 9661 2013-07-01 20:33:05Z pleblanc $

// #define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>

#include <chrono>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/uuid/uuid_io.hpp>
using namespace boost;

#include "messagequeue.h"
using namespace messageqcpp;

#include "loggingid.h"
#include "errorcodes.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "hasher.h"
#include "stlpoolallocator.h"
#include "threadnaming.h"
using namespace utils;

#include "querytele.h"
using namespace querytele;

#include "funcexp.h"
#include "jobstep.h"
#include "jlf_common.h"
#include "tupleconstantstep.h"
#include "limitedorderby.h"

#include "tupleannexstep.h"

#define QUEUE_RESERVE_SIZE 100000

namespace
{
struct TAHasher
{
  joblist::TupleAnnexStep* ts;
  utils::Hasher_r h;
  TAHasher(joblist::TupleAnnexStep* t) : ts(t)
  {
  }
  uint64_t operator()(const rowgroup::Row::Pointer&) const;
};
struct TAEq
{
  joblist::TupleAnnexStep* ts;
  TAEq(joblist::TupleAnnexStep* t) : ts(t)
  {
  }
  bool operator()(const rowgroup::Row::Pointer&, const rowgroup::Row::Pointer&) const;
};
// TODO:  Generalize these and put them back in utils/common/hasher.h
typedef tr1::unordered_set<rowgroup::Row::Pointer, TAHasher, TAEq, STLPoolAllocator<rowgroup::Row::Pointer> >
    DistinctMap_t;
};  // namespace

inline uint64_t TAHasher::operator()(const Row::Pointer& p) const
{
  Row& row = ts->row1;
  row.setPointer(p);
  return row.hash();
}

inline bool TAEq::operator()(const Row::Pointer& d1, const Row::Pointer& d2) const
{
  Row &r1 = ts->row1, &r2 = ts->row2;
  r1.setPointer(d1);
  r2.setPointer(d2);
  return r1.equals(r2);
}

namespace joblist
{
TupleAnnexStep::TupleAnnexStep(const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fInputDL(NULL)
 , fOutputDL(NULL)
 , fInputIterator(0)
 , fOutputIterator(0)
 , fRunner(0)
 , fRowsProcessed(0)
 , fRowsReturned(0)
 , fLimitStart(0)
 , fLimitCount(-1)
 , fLimitHit(false)
 , fEndOfResult(false)
 , fDistinct(false)
 , fParallelOp(false)
 , fOrderBy(NULL)
 , fConstant(NULL)
 , fFeInstance(funcexp::FuncExp::instance())
 , fJobList(jobInfo.jobListPtr)
 , fFinishedThreads(0)
{
  fExtendedInfo = "TNS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TNS;
}

TupleAnnexStep::~TupleAnnexStep()
{
  if (fParallelOp)
  {
    if (fOrderByList.size() > 0)
    {
      for (uint64_t id = 0; id < fOrderByList.size(); id++)
      {
        delete fOrderByList[id];
      }

      fOrderByList.clear();
    }

    fInputIteratorsList.clear();
    fRunnersList.clear();
  }

  if (fOrderBy)
    delete fOrderBy;

  fOrderBy = NULL;

  if (fConstant)
    delete fConstant;

  fConstant = NULL;
}

void TupleAnnexStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}

void TupleAnnexStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
  // Initialize structures used by separate workers
  uint64_t id = 1;
  fRowGroupIn = rgIn;
  fRowGroupIn.initRow(&fRowIn);
  if (fParallelOp && fOrderBy)
  {
    fOrderByList.resize(fMaxThreads + 1);
    for (id = 0; id <= fMaxThreads; id++)
    {
      // *DRRTUY use SP here?
      fOrderByList[id] = new LimitedOrderBy();
      fOrderByList[id]->distinct(fDistinct);
      fOrderByList[id]->initialize(rgIn, jobInfo, false, true);
    }
  }
  else
  {
    if (fOrderBy)
    {
      fOrderBy->distinct(fDistinct);
      fOrderBy->initialize(rgIn, jobInfo);
    }
    else if (flatOrderBy_.get())
    {
      // WIP no distinct yet
      flatOrderBy_->initialize(rgIn, jobInfo);
    }
    else if (!firstPhaseflatOrderBys_.empty())
    {
      // WIP no distinct yet
      for (auto& thread : firstPhaseflatOrderBys_)
      {
        thread->initialize(rgIn, jobInfo);
      }
      for (auto& thread : secondPhaseflatOrderBys_)
      {
        thread->initialize(rgIn, jobInfo);
      }
    }
  }

  if (fConstant == NULL)
  {
    vector<uint32_t> oids, oidsIn = rgIn.getOIDs();
    vector<uint32_t> keys, keysIn = rgIn.getKeys();
    vector<uint32_t> scale, scaleIn = rgIn.getScale();
    vector<uint32_t> precision, precisionIn = rgIn.getPrecision();
    vector<CalpontSystemCatalog::ColDataType> types, typesIn = rgIn.getColTypes();
    vector<uint32_t> csNums, csNumsIn = rgIn.getCharsetNumbers();
    vector<uint32_t> pos, posIn = rgIn.getOffsets();
    size_t n = jobInfo.nonConstDelCols.size();

    // Add all columns into output RG as keys. Can we put only keys?
    oids.insert(oids.end(), oidsIn.begin(), oidsIn.begin() + n);
    keys.insert(keys.end(), keysIn.begin(), keysIn.begin() + n);
    scale.insert(scale.end(), scaleIn.begin(), scaleIn.begin() + n);
    precision.insert(precision.end(), precisionIn.begin(), precisionIn.begin() + n);
    types.insert(types.end(), typesIn.begin(), typesIn.begin() + n);
    csNums.insert(csNums.end(), csNumsIn.begin(), csNumsIn.begin() + n);
    pos.insert(pos.end(), posIn.begin(), posIn.begin() + n + 1);

    fRowGroupOut =
        RowGroup(oids.size(), pos, oids, keys, types, csNums, scale, precision, jobInfo.stringTableThreshold);
  }
  else
  {
    fConstant->initialize(jobInfo, &rgIn);
    fRowGroupOut = fConstant->getOutputRowGroup();
  }

  fRowGroupOut.initRow(&fRowOut);
  fRowGroupDeliver = fRowGroupOut;
}

void TupleAnnexStep::run()
{
  if (fInputJobStepAssociation.outSize() == 0)
    throw logic_error("No input data list for annex step.");

  fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();

  if (fInputDL == NULL)
    throw logic_error("Input is not a RowGroup data list.");

  if (fOutputJobStepAssociation.outSize() == 0)
    throw logic_error("No output data list for annex step.");

  fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

  if (fOutputDL == NULL)
    throw logic_error("Output is not a RowGroup data list.");

  if (fDelivery == true)
  {
    fOutputIterator = fOutputDL->getIterator();
  }

  if (fParallelOp && firstPhaseflatOrderBys_.empty())
  {
    // Indexing begins with 1
    fRunnersList.resize(fMaxThreads);
    fInputIteratorsList.resize(fMaxThreads + 1);

    // Activate stats collecting before CS spawns threads.
    if (traceOn())
      dlTimes.setFirstReadTime();

    // *DRRTUY Make this block conditional
    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    for (uint32_t id = 1; id <= fMaxThreads; id++)
    {
      fInputIteratorsList[id] = fInputDL->getIterator();
      fRunnersList[id - 1] = jobstepThreadPool.invoke([this, id]() { this->execute(id); });
    }
  }
  else if (fParallelOp && !firstPhaseflatOrderBys_.empty())
  {
    // Indexing begins with 1
    fRunnersList.reserve(fMaxThreads);
    fInputIteratorsList.reserve(fMaxThreads);

    // Activate stats collecting before CS spawns threads.
    if (traceOn())
      dlTimes.setFirstReadTime();

    // *DRRTUY Make this block conditional
    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    for (uint32_t id = 0; id < fMaxThreads; ++id)
    {
      fInputIteratorsList.push_back(fInputDL->getIterator());
      fRunnersList.push_back(jobstepThreadPool.invoke(
          [this, id]()
          {
            if (id < firstPhaseflatOrderBys_.size() && firstPhaseflatOrderBys_[id].get())
              this->executeFlatOrderBy(id);
            else
            {
              // WIP print something
            }
          }));
    }
  }
  else
  {
    fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();

    if (fInputDL == NULL)
      throw logic_error("Input is not a RowGroup data list.");

    fInputIterator = fInputDL->getIterator();
    fRunner = jobstepThreadPool.invoke([this]() { this->execute(); });
  }
}

void TupleAnnexStep::join()
{
  if (fParallelOp)
  {
    jobstepThreadPool.join(fRunnersList);
  }
  else
  {
    if (fRunner)
    {
      jobstepThreadPool.join(fRunner);
    }
  }
}

uint32_t TupleAnnexStep::nextBand(messageqcpp::ByteStream& bs)
{
  RGData rgDataOut;
  bool more = false;
  uint32_t rowCount = 0;

  try
  {
    bs.restart();

    more = fOutputDL->next(fOutputIterator, &rgDataOut);

    if (more && !cancelled())
    {
      fRowGroupDeliver.setData(&rgDataOut);
      fRowGroupDeliver.serializeRGData(bs);
      rowCount = fRowGroupDeliver.getRowCount();
    }
    else
    {
      while (more)
        more = fOutputDL->next(fOutputIterator, &rgDataOut);

      fEndOfResult = true;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_DELIVERY, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::nextBand()");
    while (more)
      more = fOutputDL->next(fOutputIterator, &rgDataOut);

    fEndOfResult = true;
  }

  if (fEndOfResult)
  {
    // send an empty / error band
    rgDataOut.reinit(fRowGroupDeliver, 0);
    fRowGroupDeliver.setData(&rgDataOut);
    fRowGroupDeliver.resetRowGroup(0);
    fRowGroupDeliver.setStatus(status());
    fRowGroupDeliver.serializeRGData(bs);
  }

  return rowCount;
}

void TupleAnnexStep::execute()
{
  if (fOrderBy)
  {
    executeWithOrderBy();
  }
  else if (flatOrderBy_.get())
  {
    executeFlatOrderBy();
  }
  else if (fDistinct)
  {
    executeNoOrderByWithDistinct();
  }
  else
  {
    executeNoOrderBy();
  }

  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;
  sts.msg_type = StepTeleStats::ST_SUMMARY;
  sts.total_units_of_work = sts.units_of_work_completed = 1;
  sts.rows = fRowsReturned;
  postStepSummaryTele(sts);

  if (traceOn())
  {
    if (dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    dlTimes.setLastReadTime();
    dlTimes.setEndOfInputTime();
    printCalTrace();
  }
}

void TupleAnnexStep::execute(uint32_t id)
{
  if (fOrderByList[id])
    executeParallelOrderBy(id);
}

void TupleAnnexStep::executeNoOrderBy()
{
  utils::setThreadName("TASwoOrd");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;

  try
  {
    more = fInputDL->next(fInputIterator, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    while (more && !cancelled() && !fLimitHit)
    {
      fRowGroupIn.setData(&rgDataIn);
      fRowGroupIn.getRow(0, &fRowIn);
      // Get a new output rowgroup for each input rowgroup to preserve the rids
      rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
      fRowGroupOut.setData(&rgDataOut);
      fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
      fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
      fRowGroupOut.getRow(0, &fRowOut);

      for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled() && !fLimitHit; ++i)
      {
        // skip first limit-start rows
        if (fRowsProcessed++ < fLimitStart)
        {
          fRowIn.nextRow();
          continue;
        }

        if (UNLIKELY(fRowsReturned >= fLimitCount))
        {
          fLimitHit = true;
          fJobList->abortOnLimit((JobStep*)this);
          continue;
        }

        if (fConstant)
          fConstant->fillInConstants(fRowIn, fRowOut);
        else
          copyRow(fRowIn, &fRowOut);

        fRowGroupOut.incRowCount();

        if (++fRowsReturned < fLimitCount)
        {
          fRowOut.nextRow();
          fRowIn.nextRow();
        }
      }

      if (fRowGroupOut.getRowCount() > 0)
      {
        fOutputDL->insert(rgDataOut);
      }

      more = fInputDL->next(fInputIterator, &rgDataIn);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeNoOrderBy()");
  }

  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);

  // Bug 3136, let mini stats to be formatted if traceOn.
  fOutputDL->endOfInput();
}

void TupleAnnexStep::executeNoOrderByWithDistinct()
{
  utils::setThreadName("TASwoOrdDist");
  scoped_ptr<DistinctMap_t> distinctMap(new DistinctMap_t(10, TAHasher(this), TAEq(this)));
  vector<RGData> dataVec;
  vector<RGData> dataVecSkip;
  RGData rgDataIn;
  RGData rgDataOut;
  RGData rgDataSkip;
  RowGroup rowGroupSkip;
  Row rowSkip;
  bool more = false;

  rgDataOut.reinit(fRowGroupOut);
  fRowGroupOut.setData(&rgDataOut);
  fRowGroupOut.resetRowGroup(0);
  fRowGroupOut.getRow(0, &fRowOut);

  fRowGroupOut.initRow(&row1);
  fRowGroupOut.initRow(&row2);

  rowGroupSkip = fRowGroupOut;
  rgDataSkip.reinit(rowGroupSkip);
  rowGroupSkip.setData(&rgDataSkip);
  rowGroupSkip.resetRowGroup(0);
  rowGroupSkip.initRow(&rowSkip);
  rowGroupSkip.getRow(0, &rowSkip);

  try
  {
    more = fInputDL->next(fInputIterator, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    while (more && !cancelled() && !fLimitHit)
    {
      fRowGroupIn.setData(&rgDataIn);
      fRowGroupIn.getRow(0, &fRowIn);

      for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled() && !fLimitHit; ++i)
      {
        pair<DistinctMap_t::iterator, bool> inserted;
        Row* rowPtr;

        if (distinctMap->size() < fLimitStart)
          rowPtr = &rowSkip;
        else
          rowPtr = &fRowOut;

        if (fConstant)
          fConstant->fillInConstants(fRowIn, *rowPtr);
        else
          copyRow(fRowIn, rowPtr);

        fRowIn.nextRow();
        inserted = distinctMap->insert(rowPtr->getPointer());
        ++fRowsProcessed;

        if (inserted.second)
        {
          if (UNLIKELY(fRowsReturned >= fLimitCount))
          {
            fLimitHit = true;
            fJobList->abortOnLimit((JobStep*)this);
            break;
          }

          // skip first limit-start rows
          if (distinctMap->size() <= fLimitStart)
          {
            rowGroupSkip.incRowCount();
            rowSkip.nextRow();
            if (UNLIKELY(rowGroupSkip.getRowCount() >= rowgroup::rgCommonSize))
            {
              // allocate new RGData for skipped rows below the fLimitStart
              // offset (do not take it into account in RM assuming there
              // are few skipped rows
              dataVecSkip.push_back(rgDataSkip);
              rgDataSkip.reinit(rowGroupSkip);
              rowGroupSkip.setData(&rgDataSkip);
              rowGroupSkip.resetRowGroup(0);
              rowGroupSkip.getRow(0, &rowSkip);
            }
            continue;
          }

          ++fRowsReturned;

          fRowGroupOut.incRowCount();
          fRowOut.nextRow();

          if (UNLIKELY(fRowGroupOut.getRowCount() >= rowgroup::rgCommonSize))
          {
            dataVec.push_back(rgDataOut);
            rgDataOut.reinit(fRowGroupOut);
            fRowGroupOut.setData(&rgDataOut);
            fRowGroupOut.resetRowGroup(0);
            fRowGroupOut.getRow(0, &fRowOut);
          }
        }
      }

      more = fInputDL->next(fInputIterator, &rgDataIn);
    }

    if (fRowGroupOut.getRowCount() > 0)
      dataVec.push_back(rgDataOut);

    for (vector<RGData>::iterator i = dataVec.begin(); i != dataVec.end(); i++)
    {
      rgDataOut = *i;
      fRowGroupOut.setData(&rgDataOut);
      fOutputDL->insert(rgDataOut);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeNoOrderByWithDistinct()");
  }

  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);

  // Bug 3136, let mini stats to be formatted if traceOn.
  fOutputDL->endOfInput();
}

void TupleAnnexStep::executeWithOrderBy()
{
  utils::setThreadName("TASwOrd");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;

  try
  {
    more = fInputDL->next(fInputIterator, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    while (more && !cancelled())
    {
      fRowGroupIn.setData(&rgDataIn);
      fRowGroupIn.getRow(0, &fRowIn);

      for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled(); ++i)
      {
        fOrderBy->processRow(fRowIn);
        fRowIn.nextRow();
      }

      more = fInputDL->next(fInputIterator, &rgDataIn);
    }

    fOrderBy->finalize();

    if (!cancelled())
    {
      while (fOrderBy->getData(rgDataIn))
      {
        if (fConstant == NULL && fRowGroupOut.getColumnCount() == fRowGroupIn.getColumnCount())
        {
          rgDataOut = rgDataIn;
          fRowGroupOut.setData(&rgDataOut);
        }
        else
        {
          fRowGroupIn.setData(&rgDataIn);
          fRowGroupIn.getRow(0, &fRowIn);

          rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
          fRowGroupOut.setData(&rgDataOut);
          fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
          fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
          fRowGroupOut.getRow(0, &fRowOut);

          for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
          {
            if (fConstant)
              fConstant->fillInConstants(fRowIn, fRowOut);
            else
              copyRow(fRowIn, &fRowOut);

            fRowGroupOut.incRowCount();
            fRowOut.nextRow();
            fRowIn.nextRow();
          }
        }

        if (fRowGroupOut.getRowCount() > 0)
        {
          fRowsReturned += fRowGroupOut.getRowCount();
          fOutputDL->insert(rgDataOut);
        }
      }
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeWithOrderBy()");
  }

  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);

  // Bug 3136, let mini stats to be formatted if traceOn.
  fOutputDL->endOfInput();
}

void TupleAnnexStep::executeFlatOrderBy()
{
  utils::setThreadName("TASwOrdFlat");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;
  const uint32_t threadID = 0;
  auto start = std::chrono::steady_clock::now();
  std::cout << "it works!!" << std::endl;
  try
  {
    more = fInputDL->next(fInputIterator, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);
    auto start1 = std::chrono::steady_clock::now();

    while (more && !cancelled())
    {
      flatOrderBy_->addBatch(rgDataIn);
      more = fInputDL->next(fInputIterator, &rgDataIn);
    }
    auto end1 = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds1 = end1 - start1;
    std::cout << "flat 1Thread add batch " + std::to_string(0) + " elapsed time: " << elapsed_seconds1.count()
              << std::endl;

    if (flatOrderBy_->sortCF(threadID))
    {
      // WIP print something
    }

    flatOrderBy_->finalize();
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "flat 1Thread keys + sort " + std::to_string(0) + " elapsed time: "
              << elapsed_seconds.count() << std::endl;
    start = std::chrono::steady_clock::now();
    if (!cancelled())
    {
      while (flatOrderBy_->getData(rgDataIn, firstPhaseflatOrderBys_))
      {
        // if (fConstant == NULL && fRowGroupOut.getColumnCount() == fRowGroupIn.getColumnCount())
        {
          rgDataOut = rgDataIn;
          // WIP This is very cost inefficent to just calc rows
          fRowGroupOut.setData(&rgDataOut);
        }
        // else
        // {
        //   fRowGroupIn.setData(&rgDataIn);
        //   fRowGroupIn.getRow(0, &fRowIn);

        //   rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
        //   fRowGroupOut.setData(&rgDataOut);
        //   fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
        //   fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
        //   fRowGroupOut.getRow(0, &fRowOut);

        //   for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
        //   {
        //     if (fConstant)
        //       fConstant->fillInConstants(fRowIn, fRowOut);
        //     else
        //       copyRow(fRowIn, &fRowOut);

        //     fRowGroupOut.incRowCount();
        //     fRowOut.nextRow();
        //     fRowIn.nextRow();
        //   }
        // }

        //  WIP It is too costly to init RowGroup to use see the number of records.
        auto rows = fRowGroupOut.getRowCount();
        if (rows > 0)
        {
          fRowsReturned += rows;
          fOutputDL->insert(rgDataOut);
        }
      }
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeWithOrderByFlatOrderBy()");
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> elapsed_seconds = end - start;
  std::cout << "flat 1Thread serialize " + std::to_string(0) + " elapsed time: " << elapsed_seconds.count()
            << std::endl;
  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);

  // Bug 3136, let mini stats to be formatted if traceOn.
  fOutputDL->endOfInput();
}

void TupleAnnexStep::executeFlatOrderBy(const uint32_t id)
{
  utils::setThreadName("TASwPOrdFlat");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;
  size_t dlOffset = 0;
  assert(firstPhaseflatOrderBys_.size() > id && firstPhaseflatOrderBys_[id].get());
  try
  {
    more = fInputDL->next(fInputIteratorsList[id], &rgDataIn);
    // if (more)
    // {
    //   ++dlOffset;
    // }
    // std::cout << "flatOB id " << id << "   " << fInputIteratorsList[id] << " more " << more << std::endl;

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts;
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    while (more && !cancelled())
    {
      if (dlOffset % fMaxThreads == id)
      {
        firstPhaseflatOrderBys_[id]->addBatch(rgDataIn);
      }
      more = fInputDL->next(fInputIteratorsList[id], &rgDataIn);
      if (more)
      {
        ++dlOffset;
      }
    }
    auto start = std::chrono::steady_clock::now();

    if (firstPhaseflatOrderBys_[id]->sortCF(id))
    {
      // do something
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "execute " + std::to_string(id) + " elapsed time: " << elapsed_seconds.count() << "s\n";
    firstPhaseflatOrderBys_[id]->finalize();
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeFlatOrderBy()");
  }

  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);
  // Count finished sorting threads under mutex and run final
  // sort step when the last thread converges
  parallelOrderByMutex_.lock();
  ++fFinishedThreads;
  if (fFinishedThreads == fMaxThreads)
  {
    // WIP Replace this vector with vector of RGDatas?
    // This ref lives as long as TAS this lives.
    const SortingThreads& firstPhaseThreads = firstPhaseflatOrderBys_;
    // for (auto& thread : firstPhaseThreads)
    // {
    //   std::cout << "executeFlatOrderBy firstPhaseThreads perm size " << (*thread).getPermutation().size()
    //             << std::endl;
    // }
    auto valueRanges = calculateStats4FlatOrderBy2ndPhase(firstPhaseThreads);
    for (uint32_t i = 1; i <= fMaxThreads; ++i)
    {
      fRunnersList.push_back(
          jobstepThreadPool.invoke([this, i, valueRanges, &firstPhaseThreads]()
                                   { this->finalizeFlatOrderBy(i, valueRanges, firstPhaseThreads); }));
    }
    // Put from thread outputDLs into a common outputDL
    fRunnersList.push_back(jobstepThreadPool.invoke([this]() { this->joinOutputDLs(); }));
  }
  parallelOrderByMutex_.unlock();
}

const ValueRangesVector TupleAnnexStep::calculateStats4FlatOrderBy2ndPhase(
    const SortingThreads& sortingThreads)
{
  utils::setThreadName("TASOrdStats");
  ValueRangesVector ranges(fMaxThreads);
  vector<int64_t> valuesOnTheRight(ranges.size());
  // This algo is completely incorrect
  for (auto& sorting : sortingThreads)
  {
    const auto& perm = sorting->getPermutation();
    const auto step = perm.size() / fMaxThreads + ((perm.empty()) ? 0 : 1);
    size_t left = 0;
    size_t right = step;
    // some rightValue;
    size_t i = valuesOnTheRight.size() - 1;
    for (auto it = ranges.rbegin(); it != ranges.rend(); ++it, ++i)
    // for (size_t i = 0; i < ranges.size(); ++i)
    {
      if (it->empty())
      {
        valuesOnTheRight[i] = i;
      }
      it->push_back({left, right});
      std::cout << "stats calc left " << left << " right " << right << std::endl;
      left = right + ((perm.empty()) ? 0 : 1);
      right = (right + step > perm.size()) ? perm.size() : right + step;
    }
  }
  // for (auto& range : ranges)
  // {
  //   std::reverse(range.begin(), range.end());
  // }
  return ranges;
}

// WIP use std::function in init to assign this maybe
void TupleAnnexStep::joinOutputDLs()
{
  RGData rgDataOut;
  // bool more = true;
  std::string threadName("TAS2ndDLM");
  utils::setThreadName(threadName.c_str());

  // size_t secondPhaseFlatThreadId = 1;
  try
  {
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

    // WIP might be a costly call
    // for (; !cancelled() && secondPhaseFlatThreadId <= secondPhaseflatOrderBys_.size() + 1;
    //      ++secondPhaseFlatThreadId)
    {
      // std::cout << "joinOutputDLs in loop " << secondPhaseFlatThreadId << std::endl;
      bool more = true;
      auto outputDL = fOutputJobStepAssociation.outAt(1)->rowGroupDL();
      // std::cout << "Join id " << 1 << " " << std::hex << (uint64_t)outputDL << std::endl;

      // auto outputIterator = outputDL->getIterator();

      if (!outputDL)
      {
        throw logic_error("Output is not a RowGroup data list 1.");
      }
      while (more)
      {
        // WIP output iter is hardcoded
        more = outputDL->next(0, &rgDataOut);
        // if rgData is empty?
        // Doesn't need to set rgData b/c rowCount offset is 0
        fRowGroupOut.setData(&rgDataOut);
        auto rowCount = fRowGroupOut.getRowCount();
        // std::cout << "join " << 1 << " rowCount " << rowCount << std::endl;

        if (rowCount > 0)
        {
          fRowsReturned += rowCount;
          fOutputDL->insert(rgDataOut);
          // WIP can be costly
          rgDataOut.reinit(fRowGroupIn, 0);
          fRowGroupOut.setData(&rgDataOut);
          fRowGroupOut.setRowCount(0);
        }
      }
      // std::cout << "joinOutputDLs after " << secondPhaseFlatThreadId << " !cancelled() " << !cancelled()
      // << std::endl;
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "join serialize " + std::to_string(0) + " elapsed time: " << elapsed_seconds.count()
              << std::endl;
    start = std::chrono::steady_clock::now();

    {
      // std::cout << "joinOutputDLs in loop " << secondPhaseFlatThreadId << std::endl;
      bool more = true;
      auto outputDL = fOutputJobStepAssociation.outAt(2)->rowGroupDL();
      // std::cout << "Join id " << 2 << " " << std::hex << (uint64_t)outputDL << std::endl;

      // auto outputIterator = outputDL->getIterator();

      if (!outputDL)
      {
        throw logic_error("Output is not a RowGroup data list 1.");
      }
      while (more)
      {
        // WIP output iter is hardcoded
        more = outputDL->next(0, &rgDataOut);

        // if rgData is empty?
        // Doesn't need to set rgData b/c rowCount offset is 0
        fRowGroupOut.setData(&rgDataOut);
        auto rowCount = fRowGroupOut.getRowCount();
        // std::cout << "join " << 1 << " rowCount " << rowCount << std::endl;
        if (rowCount > 0)
        {
          // std::cout << "Join added to output id " << 2 << std::endl;
          fRowsReturned += rowCount;
          fOutputDL->insert(rgDataOut);

          rgDataOut.reinit(fRowGroupIn, 0);
          fRowGroupOut.setData(&rgDataOut);
          fRowGroupOut.setRowCount(0);
        }
      }
    }
    end = std::chrono::steady_clock::now();
    elapsed_seconds = end - start;
    std::cout << "join serialize " + std::to_string(1) + " elapsed time: " << elapsed_seconds.count()
              << std::endl;
  }
  catch (...)
  {
    // // WIP
    // for (; secondPhaseFlatThreadId <= secondPhaseflatOrderBys_.size(); ++secondPhaseFlatThreadId)
    // {
    //   auto outputDL = fOutputJobStepAssociation.outAt(secondPhaseFlatThreadId)->rowGroupDL();
    //   bool more = true;
    //   while (more)
    //   {
    //     // WIP iter number is hardcoded
    //     more = outputDL->next(0, &rgDataOut);
    //   }
    // }
    handleException(std::current_exception(), logging::ERR_IN_DELIVERY, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::joinOutputDLs()");
  }

  // WIP drain all DLs that are left
  // for (; secondPhaseFlatThreadId <= secondPhaseflatOrderBys_.size(); ++secondPhaseFlatThreadId)
  // {
  //   auto outputDL = fOutputJobStepAssociation.outAt(secondPhaseFlatThreadId)->rowGroupDL();
  //   bool more = true;
  //   while (more)
  //   {
  //     // WIP iter number is hardcoded
  //     more = outputDL->next(0, &rgDataOut);
  //   }
  // }
  // WIP might be a problem!!!
  // send an empty / error band
  fOutputDL->endOfInput();
}

void TupleAnnexStep::finalizeFlatOrderBy(const uint32_t idA, const ValueRangesVector ranges,
                                         const SortingThreads& firstPhaseThreads)
{
  // render a new permutation using argument ranges and a full range.
  // free 1st phase permutations_ when they are read.
  // Then start a new sorting.
  const uint32_t id = idA - 1;
  assert(id < secondPhaseflatOrderBys_.size());
  assert(id < firstPhaseflatOrderBys_.size());
  assert(id < ranges.size());
  assert(ranges.size() == firstPhaseflatOrderBys_.size() && ranges.size() == secondPhaseflatOrderBys_.size());
  std::string threadName("TAS2nd");
  threadName += std::to_string(id);
  // utils::setThreadName("TASwPOrdFlat2nd");
  utils::setThreadName(threadName.c_str());
  // std::cout << "enter " << threadName << std::endl;
  RGData rgDataIn;
  RGData rgDataOut;
  FlatOrderBy::PermutationVec perm;
  // WIP estimate perm size and reserve enough space
  auto firstPhaseSortingThread = firstPhaseflatOrderBys_.begin();
  auto range = ranges[id].begin();
  for (; firstPhaseSortingThread != firstPhaseflatOrderBys_.end() && range != ranges[id].end();
       ++firstPhaseSortingThread, ++range)
  {
    auto [rangeLeft, rangeRight] = *range;
    auto srcPermBegin = (*firstPhaseSortingThread)->getPermutation().begin() + rangeLeft;
    auto srcPermEnd = (*firstPhaseSortingThread)->getPermutation().begin() + rangeRight;
    // WIP hides issues
    if (srcPermBegin == srcPermEnd)
    {
      continue;
    }
    perm.insert(perm.end(), srcPermBegin, srcPermEnd);
  }
  std::cout << " finFlat id " << id << " perm size " << perm.size() << std::endl;
  // WIP clean up first phase perm vector when the last range is read from it
  FlatOrderBy::Ranges2SortQueue ranges2Sort;
  // means ranges2Sort is a pair of int64 and ::size() is uint64
  assert(perm.size() < std::numeric_limits<size_t>::max());
  ranges2Sort.push({0, perm.size()});
  // outputDL index is Nat
  auto outputDL = fOutputJobStepAssociation.outAt(id + 1)->rowGroupDL();
  // std::cout << "finFlat id " << id << " " << std::hex << (uint64_t)outputDL << std::endl;
  // This call produces a stream of sorted data in RGData format in id-th output DL.
  // This call presumes there is an equal number of threads at the first and the second phases .
  // WIP use a unique_ptr ref here and get it as an argument ?
  auto start = std::chrono::steady_clock::now();
  try
  {
    // std::cout << "try 1 id " << id << std::endl;

    if (!cancelled())
    {
      if (secondPhaseflatOrderBys_[id]->sortByColumnCFNoPerm(
              id, secondPhaseflatOrderBys_[id]->getSortingColumns(), std::move(perm), std::move(ranges2Sort),
              firstPhaseThreads))
      {
        // print something
      }
    }
    secondPhaseflatOrderBys_[id]->finalize();
    // std::cout << "try 2 id " << id << std::endl;

    // WIP
    rowgroup::RowGroup rowGroupOut{fRowGroupOut};
    while (secondPhaseflatOrderBys_[id]->getData(rgDataOut, firstPhaseflatOrderBys_))
    {
      // rgDataOut = rgDataIn;
      // WIP This is very cost inefficent to just calc rows
      // WIP replace fRowGroupOut
      rowGroupOut.setData(&rgDataOut);

      //  WIP It is too costly to init RowGroup to use see the number of records.
      // WIP replace fRowGroupOut
      auto rows = rowGroupOut.getRowCount();
      if (rows > 0)
      {
        // WIP Replace fRowsReturned with atomic counter
        // fRowsReturned += rows;
        outputDL->insert(rgDataOut);
      }
    }
    // std::cout << "in try after while " << std::to_string(id) << std::endl;
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeWithOrderByFlatOrderBy()");
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> elapsed_seconds = end - start;
  std::cout << "finalize " + std::to_string(id) + " elapsed time: " << elapsed_seconds.count() << std::endl;
  outputDL->endOfInput();
}

/*
    The m() iterates over thread's LimitedOrderBy instances,
    reverts the rules and then populates the final collection
    used for final sorting. The method uses OrderByRow that
    combination of Row::data and comparison rules.
    When m() finishes with thread's LOBs it iterates over
    final sorting collection, populates rgDataOut, then
    sends it into outputDL.
    Changing this method don't forget to make changes in
    finalizeParallelOrderBy() that is a clone.
    !!!The method doesn't set Row::baseRid
*/
void TupleAnnexStep::finalizeParallelOrderByDistinct()
{
  utils::setThreadName("TASwParOrdDistM");
  uint64_t count = 0;
  uint64_t offset = 0;
  uint32_t rowSize = 0;

  rowgroup::RGData rgDataOut;
  rgDataOut.reinit(fRowGroupOut, rowgroup::rgCommonSize);
  fRowGroupOut.setData(&rgDataOut);
  fRowGroupOut.resetRowGroup(0);
  // Calculate offset here
  fRowGroupOut.getRow(0, &fRowOut);
  ordering::SortingPQ finalPQ;
  scoped_ptr<DistinctMap_t> distinctMap(new DistinctMap_t(10, TAHasher(this), TAEq(this)));
  fRowGroupIn.initRow(&row1);
  fRowGroupIn.initRow(&row2);

  try
  {
    for (uint64_t id = 1; id <= fMaxThreads; id++)
    {
      if (cancelled())
      {
        break;
      }
      // Revert the ordering rules before we
      // add rows into the final PQ.
      fOrderByList[id]->getRule().revertRules();
      ordering::SortingPQ& currentPQ = fOrderByList[id]->getQueue();
      finalPQ.reserve(finalPQ.size() + currentPQ.size());
      pair<DistinctMap_t::iterator, bool> inserted;
      while (currentPQ.size())
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(currentPQ.top());
        inserted = distinctMap->insert(topOBRow.fData);
        if (inserted.second)
        {
          finalPQ.push(topOBRow);
        }
        currentPQ.pop();
      }
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::finalizeParallelOrderByDistinct()");
  }

  // OFFSET processing
  while (finalPQ.size() && offset < fLimitStart)
  {
    offset++;
    finalPQ.pop();
  }

  // Calculate rowSize only once
  if (finalPQ.size())
  {
    ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());
    fRowIn.setData(topOBRow.fData);
    if (!fConstant)
    {
      copyRow(fRowIn, &fRowOut);
    }
    else
    {
      fConstant->fillInConstants(fRowIn, fRowOut);
    }
    rowSize = fRowOut.getSize();
    fRowGroupOut.incRowCount();
    fRowOut.nextRow(rowSize);
    finalPQ.pop();
    count++;
  }

  if (!fConstant)
  {
    while (finalPQ.size())
    {
      if (cancelled())
      {
        break;
      }

      while (count < fLimitCount && finalPQ.size() && fRowGroupOut.getRowCount() < rowgroup::rgCommonSize)
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());

        fRowIn.setData(topOBRow.fData);
        copyRow(fRowIn, &fRowOut);
        fRowGroupOut.incRowCount();
        fRowOut.nextRow(rowSize);

        finalPQ.pop();
        count++;
        if (fRowGroupOut.getRowCount() == rowgroup::rgCommonSize)
        {
          break;
        }
      }

      if (fRowGroupOut.getRowCount() > 0)
      {
        fRowsReturned += fRowGroupOut.getRowCount();
        fOutputDL->insert(rgDataOut);
        rgDataOut.reinit(fRowGroupIn, rowgroup::rgCommonSize);
        fRowGroupOut.setData(&rgDataOut);
        fRowGroupOut.resetRowGroup(0);
        fRowGroupOut.getRow(0, &fRowOut);
      }
      else
      {
        break;
      }
    }  // end of limit bound while loop
  }
  else  // Add ConstantColumns striped earlier
  {
    while (finalPQ.size())
    {
      if (cancelled())
      {
        break;
      }

      while (count < fLimitCount && finalPQ.size() && fRowGroupOut.getRowCount() < rowgroup::rgCommonSize)
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());

        fRowIn.setData(topOBRow.fData);
        fConstant->fillInConstants(fRowIn, fRowOut);
        fRowGroupOut.incRowCount();
        fRowOut.nextRow(rowSize);

        finalPQ.pop();
        count++;
        if (fRowGroupOut.getRowCount() == rowgroup::rgCommonSize)
        {
          break;
        }
      }

      if (fRowGroupOut.getRowCount() > 0)
      {
        fRowsReturned += fRowGroupOut.getRowCount();
        fOutputDL->insert(rgDataOut);
        rgDataOut.reinit(fRowGroupOut, rowgroup::rgCommonSize);
        fRowGroupOut.setData(&rgDataOut);
        fRowGroupOut.resetRowGroup(0);
        fRowGroupOut.getRow(0, &fRowOut);
      }
      else
      {
        break;
      }
    }  // end of limit bound while loop
  }    // end of if-else

  if (fRowGroupOut.getRowCount() > 0)
  {
    fRowsReturned += fRowGroupOut.getRowCount();
    fOutputDL->insert(rgDataOut);
  }

  fOutputDL->endOfInput();

  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;
  sts.msg_type = StepTeleStats::ST_SUMMARY;
  sts.total_units_of_work = sts.units_of_work_completed = 1;
  sts.rows = fRowsReturned;
  postStepSummaryTele(sts);

  if (traceOn())
  {
    if (dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    dlTimes.setLastReadTime();
    dlTimes.setEndOfInputTime();
    printCalTrace();
  }
}

/*
    The m() iterates over thread's LimitedOrderBy instances,
    reverts the rules and then populates the final collection
    used for final sorting. The method uses OrderByRow that
    combination of Row::data and comparison rules.
    When m() finishes with thread's LOBs it iterates over
    final sorting collection, populates rgDataOut, then
    sends it into outputDL.
    Changing this method don't forget to make changes in
    finalizeParallelOrderByDistinct() that is a clone.
    !!!The method doesn't set Row::baseRid
*/
void TupleAnnexStep::finalizeParallelOrderBy()
{
  utils::setThreadName("TASwParOrdMerge");
  uint64_t count = 0;
  uint64_t offset = 0;
  uint32_t rowSize = 0;

  rowgroup::RGData rgDataOut;
  ordering::SortingPQ finalPQ;
  rgDataOut.reinit(fRowGroupOut, rowgroup::rgCommonSize);
  fRowGroupOut.setData(&rgDataOut);
  fRowGroupOut.resetRowGroup(0);
  // Calculate offset here
  fRowGroupOut.getRow(0, &fRowOut);

  try
  {
    for (uint64_t id = 1; id <= fMaxThreads; id++)
    {
      if (cancelled())
      {
        break;
      }
      // Revert the ordering rules before we
      // add rows into the final PQ.
      fOrderByList[id]->getRule().revertRules();
      ordering::SortingPQ& currentPQ = fOrderByList[id]->getQueue();
      finalPQ.reserve(currentPQ.size());
      while (currentPQ.size())
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(currentPQ.top());
        finalPQ.push(topOBRow);
        currentPQ.pop();
      }
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::finalizeParallelOrderBy()");
  }

  // OFFSET processing
  while (finalPQ.size() && offset < fLimitStart)
  {
    offset++;
    finalPQ.pop();
  }

  // Calculate rowSize only once
  if (finalPQ.size())
  {
    ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());
    fRowIn.setData(topOBRow.fData);
    if (!fConstant)
    {
      copyRow(fRowIn, &fRowOut);
    }
    else
    {
      fConstant->fillInConstants(fRowIn, fRowOut);
    }
    rowSize = fRowOut.getSize();
    fRowGroupOut.incRowCount();
    fRowOut.nextRow(rowSize);
    finalPQ.pop();
    count++;
  }

  if (!fConstant)
  {
    while (finalPQ.size())
    {
      if (cancelled())
      {
        break;
      }

      while (count < fLimitCount && finalPQ.size() && fRowGroupOut.getRowCount() < rowgroup::rgCommonSize)
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());

        fRowIn.setData(topOBRow.fData);
        copyRow(fRowIn, &fRowOut);
        fRowGroupOut.incRowCount();
        fRowOut.nextRow(rowSize);

        finalPQ.pop();
        count++;
      }

      if (fRowGroupOut.getRowCount() > 0)
      {
        fRowsReturned += fRowGroupOut.getRowCount();
        fOutputDL->insert(rgDataOut);
        rgDataOut.reinit(fRowGroupIn, rowgroup::rgCommonSize);
        fRowGroupOut.setData(&rgDataOut);
        fRowGroupOut.resetRowGroup(0);
        fRowGroupOut.getRow(0, &fRowOut);
      }
      else
      {
        break;
      }
    }  // end of limit bound while loop
  }
  else  // Add ConstantColumns striped earlier
  {
    while (finalPQ.size())
    {
      if (cancelled())
      {
        break;
      }

      while (count < fLimitCount && finalPQ.size() && fRowGroupOut.getRowCount() < rowgroup::rgCommonSize)
      {
        ordering::OrderByRow& topOBRow = const_cast<ordering::OrderByRow&>(finalPQ.top());

        fRowIn.setData(topOBRow.fData);
        fConstant->fillInConstants(fRowIn, fRowOut);
        fRowGroupOut.incRowCount();
        fRowOut.nextRow(rowSize);

        finalPQ.pop();
        count++;
        if (fRowGroupOut.getRowCount() == rowgroup::rgCommonSize)
        {
          break;
        }
      }

      if (fRowGroupOut.getRowCount() > 0)
      {
        fRowsReturned += fRowGroupOut.getRowCount();
        fOutputDL->insert(rgDataOut);
        rgDataOut.reinit(fRowGroupOut, rowgroup::rgCommonSize);
        fRowGroupOut.setData(&rgDataOut);
        fRowGroupOut.resetRowGroup(0);
        fRowGroupOut.getRow(0, &fRowOut);
      }
      else
      {
        break;
      }
    }  // end of limit bound while loop
  }    // end of if-else

  if (fRowGroupOut.getRowCount() > 0)
  {
    fRowsReturned += fRowGroupOut.getRowCount();
    fOutputDL->insert(rgDataOut);
  }

  fOutputDL->endOfInput();

  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;
  sts.msg_type = StepTeleStats::ST_SUMMARY;
  sts.total_units_of_work = sts.units_of_work_completed = 1;
  sts.rows = fRowsReturned;
  postStepSummaryTele(sts);

  if (traceOn())
  {
    if (dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    dlTimes.setLastReadTime();
    dlTimes.setEndOfInputTime();
    printCalTrace();
  }
}

void TupleAnnexStep::executeParallelOrderBy(uint64_t id)
{
  utils::setThreadName("TASwParOrd");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;
  uint64_t dlOffset = 0;
  uint32_t rowSize = 0;

  uint64_t rowCount = 0;
  uint64_t doubleRGSize = 2 * rowgroup::rgCommonSize;
  rowgroup::Row r = fRowIn;
  rowgroup::RowGroup rg = fRowGroupIn;
  rg.initRow(&r);
  LimitedOrderBy* limOrderBy = fOrderByList[id];
  ordering::SortingPQ& currentPQ = limOrderBy->getQueue();
  if (limOrderBy->getLimitCount() < QUEUE_RESERVE_SIZE)
  {
    currentPQ.reserve(limOrderBy->getLimitCount());
  }
  else
  {
    currentPQ.reserve(QUEUE_RESERVE_SIZE);
  }

  try
  {
    more = fInputDL->next(fInputIteratorsList[id], &rgDataIn);
    if (more)
      dlOffset++;

    while (more && !cancelled())
    {
      if (dlOffset % fMaxThreads == id - 1)
      {
        if (cancelled())
          break;

        if (currentPQ.capacity() - currentPQ.size() < doubleRGSize)
        {
          currentPQ.reserve(QUEUE_RESERVE_SIZE);
        }

        rg.setData(&rgDataIn);
        rg.getRow(0, &r);
        if (!rowSize)
        {
          rowSize = r.getSize();
        }
        rowCount = rg.getRowCount();

        for (uint64_t i = 0; i < rowCount; ++i)
        {
          limOrderBy->processRow(r);
          r.nextRow(rowSize);
        }
      }

      // *DRRTUY Implement a method to skip elements in FIFO
      more = fInputDL->next(fInputIteratorsList[id], &rgDataIn);
      if (more)
        dlOffset++;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexStep::executeParallelOrderBy()");
  }

  // read out the input DL
  while (more)
    more = fInputDL->next(fInputIteratorsList[id], &rgDataIn);

  // Count finished sorting threads under mutex and run final
  // sort step when the last thread converges
  fParallelFinalizeMutex.lock();
  fFinishedThreads++;
  if (fFinishedThreads == fMaxThreads)
  {
    fParallelFinalizeMutex.unlock();
    if (fDistinct)
    {
      finalizeParallelOrderByDistinct();
    }
    else
    {
      finalizeParallelOrderBy();
    }
  }
  else
  {
    fParallelFinalizeMutex.unlock();
  }
}

const RowGroup& TupleAnnexStep::getOutputRowGroup() const
{
  return fRowGroupOut;
}

const RowGroup& TupleAnnexStep::getDeliveredRowGroup() const
{
  return fRowGroupDeliver;
}

void TupleAnnexStep::deliverStringTableRowGroup(bool b)
{
  fRowGroupOut.setUseStringTable(b);
  fRowGroupDeliver.setUseStringTable(b);
}

bool TupleAnnexStep::deliverStringTableRowGroup() const
{
  idbassert(fRowGroupOut.usesStringTable() == fRowGroupDeliver.usesStringTable());
  return fRowGroupDeliver.usesStringTable();
}

const string TupleAnnexStep::toString() const
{
  ostringstream oss;
  oss << "AnnexStep ";
  oss << "  ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  if (fOrderBy)
    oss << "    " << fOrderBy->toString();

  if (fConstant)
    oss << "    " << fConstant->toString();

  oss << endl;

  return oss.str();
}

void TupleAnnexStep::printCalTrace()
{
  time_t t = time(0);
  char timeString[50];
  ctime_r(&t, timeString);
  timeString[strlen(timeString) - 1] = '\0';
  ostringstream logStr;
  logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString
         << "; total rows returned-" << fRowsReturned << endl
         << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
         << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
         << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
         << "\tJob completion status " << status() << endl;
  logEnd(logStr.str().c_str());
  fExtendedInfo += logStr.str();
  formatMiniStats();
}

void TupleAnnexStep::formatMiniStats()
{
  ostringstream oss;
  oss << "TNS ";
  oss << "UM "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- " << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
      << fRowsReturned << " ";
  fMiniInfo += oss.str();
}

}  // namespace joblist
