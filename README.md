
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class ProfilePipeline:
    def __init__(
        self,
        name="Mariana Costa",
        role="data and more stuff",
        education="B.E. Production Engineering - UFRN (Brazil)",
        interests=("music", "HQ", "data literacy", "french"),
        stack=("PySpark", "Databricks", "Terraform", "SQL", "Python"),
        app_name="mrncstt-profile",
    ):
        self.name = name
        self.role = role
        self.education = education
        self.interests = list(interests)
        self.stack = list(stack)
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def build_profile(self):
        schema = StructType([
            StructField("name", StringType()),
            StructField("role", StringType()),
            StructField("education", StringType()),
            StructField("interests", ArrayType(StringType())),
            StructField("stack", ArrayType(StringType())),
        ])

        data = [(self.name, self.role, self.education, self.interests, self.stack)]

        self.df_profile = (
            self.spark.createDataFrame(data, schema=schema)
            .withColumn("interests", F.array_join("interests", ", "))
            .withColumn("stack", F.array_join("stack", " | "))
            .withColumn("updated_at", F.current_date())
        )

    def stop(self):
        self.spark.stop()


def process_pipeline():
    pipeline = ProfilePipeline()
    pipeline.build_profile()
    display(pipeline.df_profile)
    pipeline.stop()


process_pipeline()
```

### `display(pipeline.df_profile)`

| name | role | education | interests | stack | updated_at |
|------|------|-----------|-----------|-------|------------|
| Mariana Costa | data and more stuff | B.E. Production Engineering - UFRN (Brazil) | music, HQ, data literacy, french | PySpark \| Databricks \| Terraform \| SQL \| Python | 2026-05-04 |

---

#### Latest from the blog

<!-- BLOG-POST:START -->
[Accessibility Service wasn't made for this](https://mrncstt.github.io/posts/accessibility-service-nao-era-pra-isso/)
<!-- BLOG-POST:END -->

---

[Blog](https://mrncstt.github.io) - [LinkedIn](https://www.linkedin.com/in/mrncstt/) - [GitHub](https://github.com/mrncstt)
