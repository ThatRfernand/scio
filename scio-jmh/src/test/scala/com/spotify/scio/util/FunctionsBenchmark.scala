package com.spotify.scio.util

import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import com.twitter.algebird.{Monoid, Semigroup}
import org.apache.beam.sdk.testing.CombineFnTester
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class FunctionsBenchmark {

  val input = Lists.newArrayList((1 to 100).asJava)
  val output = (1 to 100).sum

  val aggregateFn = Functions.aggregateFn[Int, Int](0)(_ + _, _ + _)
  val combineFn = Functions.combineFn[Int, Int](identity, _ + _, _ + _)
  val reduceFn = Functions.reduceFn((x: Int, y: Int) => x + y)
  val sgFn = Functions.reduceFn(Semigroup.intSemigroup)
  val monFn = Functions.reduceFn(Monoid.intMonoid)

  def test(fn: CombineFn[Int, _, Int], output: Int): Int = {
    CombineFnTester.testCombineFn(fn, input, output)
    output
  }

  @Benchmark def benchAggregate: Int = test(aggregateFn, output)
  @Benchmark def benchCombine: Int = test(combineFn, output)
  @Benchmark def benchReduce: Int = test(reduceFn, output)
  @Benchmark def benchSemigroup: Int = test(sgFn, output)
  @Benchmark def benchMonoid: Int = test(monFn, output)

}
