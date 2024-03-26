#include "timing.hh"

#include <iomanip>
#include <library/utils.hh>

namespace timing {

Timing::Interval::Interval(str&& descriptor)
    : descriptor_(std::forward<str>(descriptor)) {
  clock_id_ = CLOCK_MONOTONIC;
  clear();
}

void Timing::Interval::start() {
  lib_assert(clock_gettime(clock_id_, &time_start_) == 0,
             "calling clock_gettime failed");
}

void Timing::Interval::stop() {
  struct timespec time_now;  // NOLINT
  lib_assert(clock_gettime(clock_id_, &time_now) == 0,
             "calling clock_gettime failed");

  time_ = time_now - time_start_ + time_;
}

void Timing::Interval::clear() { time_.tv_sec = time_.tv_nsec = 0; }

void Timing::Interval::add(const Timing::IntervalPtr& t2) {
  time_ = time_ + t2->time_;
}

f64 Timing::Interval::get_ms() const {
  return time_.tv_nsec / 1000000.0 + time_.tv_sec * 1000.0;  // NOLINT
}

Timing::IntervalPtr Timing::create_enroll(str&& descriptor) {
  IntervalPtr interval = std::make_shared<Interval>(std::move(descriptor));
  intervals_.push_back(interval);

  return interval;
}

Timing::json Timing::to_json() const {
  json out;

  for (auto& interval : intervals_) {
    out[interval->descriptor_] = interval->get_ms();
  }

  return out;
}

std::ostream& operator<<(std::ostream& os, const Timing& timing) {
  return os << timing.to_json().dump();
}

struct timespec operator+(const struct timespec& ts1,
                          const struct timespec& ts2) {
  struct timespec res;  // NOLINT

  if (ts1.tv_sec >= 0 && ts2.tv_sec >= 0) {
    res.tv_sec = ts1.tv_sec + ts2.tv_sec;
    res.tv_nsec = ts1.tv_nsec + ts2.tv_nsec;

    if (res.tv_nsec > 1000000000) {
      res.tv_nsec -= 1000000000;
      res.tv_sec += 1;
    }
  } else {
    std::perror("timing call to operator+ failed");
    std::abort();
  }

  return res;
}

struct timespec operator-(const struct timespec& ts1,
                          const struct timespec& ts2) {
  struct timespec res;  // NOLINT

  if (ts1.tv_sec >= 0 && ts2.tv_sec >= 0) {
    res.tv_sec = ts1.tv_sec - ts2.tv_sec;
    res.tv_nsec = ts1.tv_nsec - ts2.tv_nsec;

    if (res.tv_nsec < 0) {
      res.tv_nsec += 1000000000;
      res.tv_sec -= 1;
    }
  } else {
    std::perror("timing call to operator- failed");
    std::abort();
  }

  return res;
}

std::ostream& operator<<(std::ostream& os, const struct timespec& ts) {
  char prev;

  os << ts.tv_sec << ".";
  prev = os.fill('0');
  os << std::setw(9) << ts.tv_nsec;
  os.fill(prev);

  return os;
}

}  // namespace timing
