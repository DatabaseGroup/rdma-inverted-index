#ifndef TIMING_TIMING_HH
#define TIMING_TIMING_HH

#include <ctime>
#include <library/types.hh>
#include <ostream>

#include "extern/nlohmann/json.hh"

namespace timing {

class Timing {
public:
  struct Interval {
    str descriptor_;

    clockid_t clock_id_;
    struct timespec time_ {};
    struct timespec time_start_ {};

    explicit Interval(str&& descriptor);

    void start();
    void stop();
    void clear();
    void add(const s_ptr<Interval>& t2);

    f64 get_ms() const;
  };

public:
  using IntervalPtr = s_ptr<Interval>;
  using json = nlohmann::json;

  IntervalPtr create_enroll(str&& descriptor);
  static inline void start(IntervalPtr& interval) { interval->start(); }
  static inline void stop(IntervalPtr& interval) { interval->stop(); };
  static inline void clear(IntervalPtr& interval) { interval->clear(); };
  static inline f64 get_ms(timespec t) {
    return t.tv_nsec / 1000000.0 + t.tv_sec * 1000.0;  // NOLINT
  }

  json to_json() const;
  friend std::ostream& operator<<(std::ostream& os, const Timing& timing);

private:
  vec<IntervalPtr> intervals_;
};

struct timespec operator+(const struct timespec& ts1,
                          const struct timespec& ts2);

struct timespec operator-(const struct timespec& ts1,
                          const struct timespec& ts2);

std::ostream& operator<<(std::ostream& os, const struct timespec& ts);

}  // namespace timing

#endif  // TIMING_TIMING_HH
