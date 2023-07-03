#pragma once

#include <queue>
#include <boost/thread/thread.hpp>

#include "jobstep.h"
#include "limitedorderby.h"

namespace joblist
{
class TupleConstantStep;
class LimitedOrderBy;
}  // namespace joblist

namespace joblist
{
/** @brief class TupleAnnexConstantStep
 *
 */
class TupleAnnexConstantStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleAnnexConstantStep constructor
   */
  TupleAnnexConstantStep(const JobInfo& jobInfo);
  // Copy ctor to have a class mutex
  TupleAnnexConstantStep(const TupleAnnexConstantStep& copy);

  /** @brief TupleAnnexConstantStep destructor
   */
  ~TupleAnnexConstantStep();

  // inherited methods
  void run();
  void join();
  const std::string toString() const;

  /** @brief TupleJobStep's pure virtual methods
   */
  const rowgroup::RowGroup& getOutputRowGroup() const;
  void setOutputRowGroup(const rowgroup::RowGroup&);

  /** @brief TupleDeliveryStep's pure virtual methods
   */
  uint32_t nextBand(messageqcpp::ByteStream& bs);
  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;

  void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

  void setLimit(uint64_t s, uint64_t c)
  {
    fLimitStart = s;
    fLimitCount = c;
  }

  void setMaxThreads(uint32_t number)
  {
    fMaxThreads = number;
  }

  virtual bool stringTableFriendly()
  {
    return true;
  }

  rowgroup::Row row1, row2;  // scratch space for distinct comparisons todo: make them private

 protected:
  void execute();
  void executeNoOrderBy();
  void formatMiniStats();
  void printCalTrace();

  // input/output rowgroup and row
  rowgroup::RowGroup fRowGroupIn;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::RowGroup fRowGroupDeliver;
  rowgroup::RGData fRgDataOut;
  rowgroup::Row fRowIn;
  rowgroup::Row fRowOut;

  // for datalist
  RowGroupDL* fInputDL;
  RowGroupDL* fOutputDL;
  uint64_t fInputIterator;
  std::vector<uint64_t> fInputIteratorsList;
  uint64_t fOutputIterator;

  uint64_t fRunner;  // thread pool handle

  uint64_t fRowsProcessed;
  uint64_t fRowsReturned;
  uint64_t fLimitStart;
  uint64_t fLimitCount;
  uint64_t fMaxThreads;
  bool fLimitHit;
  bool fEndOfResult;
  //bool fParallelOp;

  funcexp::FuncExp* fFeInstance;
  JobList* fJobList;

  std::vector<uint64_t> fRunnersList;
  uint16_t fFinishedThreads;
  boost::mutex fParallelFinalizeMutex;
};

}  // namespace joblist

