Tools Contained in this Repository
==================================

- __cg-producer__: Consumes PyPI packaging information from a kafka topic and
  produces call graphs into another kafka topic.
- __kafka-filter-pypi__: Consumes PyPI packaging information in the
  [Warehouse](https://warehouse.readthedocs.io/) format from a kafka topic
  and produces unique package-version tuples into another kafka topic.
