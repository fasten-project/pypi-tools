Tools Contained in this Repository
==================================

- __kafka-filter-pypi__: Consumes PyPI packaging information in the
  [Warehouse](https://warehouse.readthedocs.io/) format from a kafka topic
  and produces unique package-version tuples into another kafka topic.
- __cg-producer__: Consumes PyPI packaging information from a kafka topic and
  produces call graphs into another kafka topic.

Pipeline
========

1. Use `kafka-filter-pypi` to extract useful PyPI packaging information to an
   output topic.
2. Use the `kafka-filter-pypi` output topic as an input topic for `cg-producer`
   in order to generate the call graphs of the packages identified.
