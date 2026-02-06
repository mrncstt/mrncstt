

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
            StructField("digital_garden", StringType()),
            StructField("contact", StringType()),
        ])

        data = [(
            "Mariana Costa",
            "Data Engineer",
            "B.E. Production Engineering - UFRN (Brazil)",
            ["music", "HQ", "data literacy", "french"],
            ["PySpark", "Databricks", "SQL", "Python", "Power BI", "Tableau", "Qlik"],
            "mrncstt.github.io",
            "linkedin.com/in/mrncstt",
        )]

        self.df_profile = (
            self.spark.createDataFrame(data, schema=schema)
            .withColumn("interests", F.array_join("interests", ", "))
            .withColumn("stack", F.array_join("stack", " | "))
            .withColumn("updated_at", F.current_date())
        )

    def build_blog(self):
        schema = StructType([
            StructField("title", StringType()),
            StructField("url", StringType()),
        ])

        data = [
            ("Export your LinkedIn saved posts with Selenium and Beautiful Soup",
             "https://mrncstt.github.io/posts/export_linkedin_saved_posts_selenium_bs4/"),
            ("Seeking insights from a recording using Google Cloud Speech-to-Text, Google Colab and ChatGPT",
             "https://mrncstt.github.io/posts/seeking_insights_from_a_recording_using_google_cloud_speech_to_text_google_colab_and_chatgpt/"),
            ("Enhance your BI skills: people to follow for Tableau, Qlik, and Power BI",
             "https://mrncstt.github.io/posts/bi_people_follow/"),
            ("Buenos Aires for 33 days: a practical guide",
             "https://mrncstt.github.io/posts/tips_buenos_aires/"),
            ("Make Over Monday 2022 W/35",
             "https://mrncstt.github.io/posts/make_over_monday_2022_w_35/"),
            ("How do I keep myself updated about data, product management and productivity",
             "https://mrncstt.github.io/posts/stay-updated-data-product-management-productivity/"),
            ("Podcasts to learn French",
             "https://mrncstt.github.io/posts/podcasts_French/"),
            ("What I learned as a hackathon mentor",
             "https://mrncstt.github.io/posts/What_I_learned_from_being_a_mentor/"),
            ("Turning memorial comments into a Qlik word cloud",
             "https://mrncstt.github.io/posts/Word_Cloud_with_Qlik/"),
            ("What I learned from teaching my first course",
             "https://mrncstt.github.io/posts/What_I_Learned_from_Teaching/"),
        ]

        self.df_blog = (
            self.spark.createDataFrame(data, schema=schema)
            .withColumn("category", F.when(F.lower("title").contains("french"), "languages")
                .when(F.lower("title").contains("bi") | F.lower("title").contains("tableau")
                      | F.lower("title").contains("qlik") | F.lower("title").contains("power bi"), "data viz")
                .when(F.lower("title").contains("selenium") | F.lower("title").contains("speech-to-text"), "tech")
                .when(F.lower("title").contains("mentor") | F.lower("title").contains("teaching"), "career")
                .otherwise("misc"))
        )

    def display(self):
        display(self.df_profile)
        display(self.df_blog)

    def stop(self):
        self.spark.stop()


pipeline = ProfilePipeline()
pipeline.build_profile()
pipeline.build_blog()
pipeline.display()
pipeline.stop()
```

### `display(pipeline.df_profile)`

| name | role | education | interests | stack | digital_garden | contact | updated_at |
|------|------|-----------|-----------|-------|----------------|---------|------------|
| Mariana Costa | Data Engineer | B.E. Production Engineering - UFRN (Brazil) | music, HQ, data literacy, french | PySpark \| Databricks \| SQL \| Python \| Power BI \| Tableau \| Qlik | [mrncstt.github.io](https://mrncstt.github.io) | [linkedin.com/in/mrncstt](https://www.linkedin.com/in/mrncstt) | 2026-02-06 |

### `display(pipeline.df_blog)`

| title | url | category |
|-------|-----|----------|
| Export your LinkedIn saved posts with Selenium and Beautiful Soup | [link](https://mrncstt.github.io/posts/export_linkedin_saved_posts_selenium_bs4/) | tech |
| Seeking insights from a recording using Google Cloud Speech-to-Text, Google Colab and ChatGPT | [link](https://mrncstt.github.io/posts/seeking_insights_from_a_recording_using_google_cloud_speech_to_text_google_colab_and_chatgpt/) | tech |
| Enhance your BI skills: people to follow for Tableau, Qlik, and Power BI | [link](https://mrncstt.github.io/posts/bi_people_follow/) | data viz |
| Buenos Aires for 33 days: a practical guide | [link](https://mrncstt.github.io/posts/tips_buenos_aires/) | misc |
| Make Over Monday 2022 W/35 | [link](https://mrncstt.github.io/posts/make_over_monday_2022_w_35/) | misc |
| How do I keep myself updated about data, product management and productivity | [link](https://mrncstt.github.io/posts/stay-updated-data-product-management-productivity/) | misc |
| Podcasts to learn French | [link](https://mrncstt.github.io/posts/podcasts_French/) | languages |
| What I learned as a hackathon mentor | [link](https://mrncstt.github.io/posts/What_I_learned_from_being_a_mentor/) | career |
| Turning memorial comments into a Qlik word cloud | [link](https://mrncstt.github.io/posts/Word_Cloud_with_Qlik/) | data viz |
| What I learned from teaching my first course | [link](https://mrncstt.github.io/posts/What_I_Learned_from_Teaching/) | career |
