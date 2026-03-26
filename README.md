# Flink RoaringBitmap UDFs

A collection of [RoaringBitmap](https://roaringbitmap.org/) user-defined functions (UDFs) for [Apache Flink](https://flink.apache.org/).

This project provides a temporary solution for RoaringBitmap support in Flink SQL before [FLIP-556](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=399278679) is formally supported in Flink. Before that, you can use the published UDF JARs with Flink 1.18 through Flink 2.x.

## Getting Started

### Prerequisites

- Apache Flink 1.18 or later (including Flink 2.x)
- Java 8 or later

### Installation

Download the latest release JAR from the [Releases](https://github.com/flink-extended/flink-roaringbitmap/releases) page, and add it to your Flink `lib/` directory or include it in your job's classpath.

### Usage

Register the UDFs in your Flink SQL environment:

```sql
-- Create a RoaringBitmap from a column of integer values
SELECT BITMAP_BUILD_AGG(user_id) FROM events GROUP BY dimension;

-- Get the cardinality of a RoaringBitmap
SELECT BITMAP_CARDINALITY(bitmap) FROM bitmaps;

-- Compute the union (OR) of multiple RoaringBitmaps
SELECT BITMAP_OR_AGG(bitmap) FROM bitmaps GROUP BY dimension;

```bash
mvn clean package
```

## License

This project is licensed under the [Apache License 2.0](LICENSE).
