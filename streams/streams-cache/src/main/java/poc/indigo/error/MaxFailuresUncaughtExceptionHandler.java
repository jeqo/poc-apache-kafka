package poc.indigo.error;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class MaxFailuresUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

  final int maxFailures;
  final long maxTimeIntervalMillis;
  private Instant previousErrorTime;
  private int currentFailureCount;

  public MaxFailuresUncaughtExceptionHandler(final int maxFailures, final long maxTimeIntervalMillis) {
    this.maxFailures = maxFailures;
    this.maxTimeIntervalMillis = maxTimeIntervalMillis;
  }

  @Override
  public StreamThreadExceptionResponse handle(final Throwable throwable) {
    currentFailureCount++;
    Instant currentErrorTime = Instant.now();

    if (previousErrorTime == null) {
      previousErrorTime = currentErrorTime;
    }

    long millisBetweenFailure = ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime);

    if (currentFailureCount >= maxFailures) {
      if (millisBetweenFailure <= maxTimeIntervalMillis) {
        return SHUTDOWN_APPLICATION;
      } else {
        currentFailureCount = 0;
        previousErrorTime = null;
      }
    }
    return REPLACE_THREAD;
  }
}
