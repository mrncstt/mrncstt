
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class ProfilePipeline:
    def __init__(self, app_name="mrncstt-profile"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def build_profile(self):
        schema = StructType([
            StructField("name", StringType()),
            StructField("role", StringType()),
            StructField("education", StringType()),
            StructField("interests", ArrayType(StringType())),
            StructField("stack", ArrayType(StringType())),
        ])

        data = [(
            "Mariana Costa",
            "data and more stuff",
            "B.E. Production Engineering - UFRN (Brazil)",
            ["music", "HQ", "data literacy", "french"],
            ["PySpark", "Databricks", "Terraform", "SQL", "Python"],
        )]

        self.df_profile = (
            self.spark.createDataFrame(data, schema=schema)
            .withColumn("interests", F.array_join("interests", ", "))
            .withColumn("stack", F.array_join("stack", " | "))
            .withColumn("updated_at", F.current_date())
        )

    def display(self):
        display(self.df_profile)

    def stop(self):
        self.spark.stop()


pipeline = ProfilePipeline()
pipeline.build_profile()
pipeline.display()
pipeline.stop()
```

### `display(pipeline.df_profile)`

| name | role | education | interests | stack | updated_at |
|------|------|-----------|-----------|-------|------------|
| Mariana Costa | data and more stuff | B.E. Production Engineering - UFRN (Brazil) | music, HQ, data literacy, french | PySpark \| Databricks \| Terraform \| SQL \| Python | 2026-03-06 |

---

#### Latest from the blog

<!-- BLOG-POST:START -->
[Accessibility Service não era pra isso](https://mrncstt.github.io/posts/accessibility-service-nao-era-pra-isso/)
<!-- BLOG-POST:END -->

---

[Blog](https://mrncstt.github.io) - [LinkedIn](https://www.linkedin.com/in/mrncstt/) - [GitHub](https://github.com/mrncstt)
