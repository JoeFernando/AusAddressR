context("Building GNAF DB")

test_that("buildGNAF builds the GNAF DB in SQLite", {
  expected = T

  actual = buildGNAF('GNAF')

  expect_identical(actual, expected)
})
