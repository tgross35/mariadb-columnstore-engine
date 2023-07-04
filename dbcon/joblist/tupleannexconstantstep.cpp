//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
#include <tr1/unordered_set>
using namespace std;

#include <boost/shared_ptr.hpp>

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

#include "tupleannexconstantstep.h"

#define QUEUE_RESERVE_SIZE 100000

namespace
{
struct TAHasher
{
  joblist::TupleAnnexConstantStep* ts;
  utils::Hasher_r h;
  TAHasher(joblist::TupleAnnexConstantStep* t) : ts(t)
  {
  }
  uint64_t operator()(const rowgroup::Row::Pointer&) const;
};
struct TAEq
{
  joblist::TupleAnnexConstantStep* ts;
  TAEq(joblist::TupleAnnexConstantStep* t) : ts(t)
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
TupleAnnexConstantStep::TupleAnnexConstantStep(const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fJobList(jobInfo.jobListPtr)
{
  fExtendedInfo = "TNS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TNS;
}

TupleAnnexConstantStep::~TupleAnnexConstantStep()
{

}

void TupleAnnexConstantStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}

void TupleAnnexConstantStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
  // Initialize structures used by separate workers
  fRowGroupIn = rgIn;
  fRowGroupIn.initRow(&fRowIn);

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



  fRowGroupOut.initRow(&fRowOut);
  fRowGroupDeliver = fRowGroupOut;
}

void TupleAnnexConstantStep::run()
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


  fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
  if (fInputDL == NULL)
    throw logic_error("Input is not a RowGroup data list.");

  fInputIterator = fInputDL->getIterator();
  fRunner = jobstepThreadPool.invoke([this](){this->execute();});
}

void TupleAnnexConstantStep::join()
{
  if (fRunner)
  {
    jobstepThreadPool.join(fRunner);
  }
}

uint32_t TupleAnnexConstantStep::nextBand(messageqcpp::ByteStream& bs)
{
  bool more = false;
  uint32_t rowCount = 0;

  try
  {
    bs.restart();

    more = fOutputDL->next(fOutputIterator, &fRgDataOut);

    if (more && !cancelled())
    {
      fRowGroupDeliver.setData(&fRgDataOut);
      fRowGroupDeliver.serializeRGData(bs);
      rowCount = fRowGroupDeliver.getRowCount();
    }
    else
    {
      while (more)
        more = fOutputDL->next(fOutputIterator, &fRgDataOut);

      fEndOfResult = true;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_DELIVERY, logging::ERR_ALWAYS_CRITICAL,
                    "TupleAnnexConstantStep::nextBand()");
    while (more)
      more = fOutputDL->next(fOutputIterator, &fRgDataOut);

    fEndOfResult = true;
  }

  if (fEndOfResult)
  {
    // send an empty / error band
    fRgDataOut.reinit(fRowGroupDeliver, 0);
    fRowGroupDeliver.setData(&fRgDataOut);
    fRowGroupDeliver.resetRowGroup(0);
    fRowGroupDeliver.setStatus(status());
    fRowGroupDeliver.serializeRGData(bs);
  }

  return rowCount;
}

void TupleAnnexConstantStep::execute()
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

        //if (fConstant)
        //  fConstant->fillInConstants(fRowIn, fRowOut);
        //else
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
                    "TupleAnnexConstantStep::execute()");
  }

  while (more)
    more = fInputDL->next(fInputIterator, &rgDataIn);

  // Bug 3136, let mini stats to be formatted if traceOn.
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

const RowGroup& TupleAnnexConstantStep::getOutputRowGroup() const
{
  return fRowGroupOut;
}

const RowGroup& TupleAnnexConstantStep::getDeliveredRowGroup() const
{
  return fRowGroupDeliver;
}

void TupleAnnexConstantStep::deliverStringTableRowGroup(bool b)
{
  fRowGroupOut.setUseStringTable(b);
  fRowGroupDeliver.setUseStringTable(b);
}

bool TupleAnnexConstantStep::deliverStringTableRowGroup() const
{
  idbassert(fRowGroupOut.usesStringTable() == fRowGroupDeliver.usesStringTable());
  return fRowGroupDeliver.usesStringTable();
}

const string TupleAnnexConstantStep::toString() const
{
  ostringstream oss;
  oss << "AnnexConstantStep ";
  oss << "  ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleAnnexConstantStep::printCalTrace()
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

void TupleAnnexConstantStep::formatMiniStats()
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
