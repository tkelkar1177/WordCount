package Simulations

import MapReduce.WordCountAnalytics.TypeCounter.config
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WordCountTest extends AnyFlatSpec with Matchers {
  behavior of "configuration parameters module"

  it should "obtain the ERROR log string" in {
    config.getString("wordCount.logType.error") shouldBe "ERROR"
  }

  it should "obtain the INFO log string" in {
    config.getString("wordCount.logType.info") shouldBe "INFO"
  }

  it should "obtain the WARN log string" in {
    config.getString("wordCount.logType.warn") shouldBe "WARN"
  }

  it should "obtain the DEBUG log string" in {
    config.getString("wordCount.logType.debug") shouldBe "DEBUG"
  }

  it should "obtain the Timestamp regex string" in {
    config.getLong("wordCount.timestampPattern") shouldBe "[0-2][0-3]:[0-5][0-9]:[0-5][0-9]:[0-9][0-9][0-9]"
  }

  it should "obtain the Random injected string regex pattern" in {
    config.getLong("wordCount.randomPattern") shouldBe "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
  }
}