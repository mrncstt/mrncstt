<!-- Heading -->
<h3 align="center"><img src = "https://raw.githubusercontent.com/MartinHeinz/MartinHeinz/master/wave.gif" width = 30px> Hi there!</h3>

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

class ProfileETL:
    def __init__(self, app_name="ProfileApp"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
    def load(self, data, schema):
        self.profiles_df = self.spark.createDataFrame(data, schema=schema)
    def explode_blog_posts(self):
        assert hasattr(self, "profiles_df")
        self.blog_df = (
            self.profiles_df
            .select("name", F.explode(F.arrays_zip("blog_posts", "blog_links")).alias("blog"))
            .select(
                "name",
                F.col("blog.blog_posts").alias("title"),
                F.col("blog.blog_links").alias("url")
            )
        )
        self.profiles_df = self.profiles_df.drop("blog_posts", "blog_links")
    def display(self, truncate=False):
        assert all(hasattr(self, c) for c in ("profiles_df", "blog_df"))
        fallback = lambda df: df.show(truncate=truncate)
        globals().get("display", fallback)(self.profiles_df)
        globals().get("display", fallback)(self.blog_df)
    def stop(self):
        self.spark.stop()

schema = StructType([
    StructField("name", StringType()),
    StructField("occupation", StringType()),
    StructField("education", StringType()),
    StructField("interests", ArrayType(StringType())),
    StructField("blog_posts", ArrayType(StringType())),
    StructField("digital_garden", StringType()),
    StructField("contact_info", StringType()),
    StructField("linkedin", StringType()),
    StructField("dev_to", StringType()),
    StructField("medium", StringType()),
    StructField("blog_links", ArrayType(StringType())),
])

data = [(
    "Mariana",
    "Data analytics enthusiast",
    "B.E. Production Engineering — Federal University of Rio Grande do Norte (Brazil)",
    ["music", "HQ", "data literacy"],
    [
        "Automating LinkedIn post extraction using Selenium and BeautifulSoup",
        "Seeking insights from a recording using Google Cloud Speech-to-Text, Google Colab and ChatGPT",
        "Enhance Your Business Intelligence Skills: Influencers to Follow for Tableau, Qlik, and Power BI Content",
        "How was my trip to Buenos Aires",
        "Make Over Monday – Top 10 Countries in Military Spending",
    ],
    "I try to write regular blog posts; most of them live on my site mrncstt.github.io",
    "You can reach me on social media.",
    "https://www.linkedin.com/in/mrncstt",
    "https://dev.to/mrncstt",
    "https://medium.com/@mrncstt",
    [
        "https://mrncstt.github.io/posts/automatinglinkedin_post_extraction/",
        "https://mrncstt.github.io/posts/seeking_insights_from_a_recording_using_google_cloud_speech_to_text_google_colab_and_chatgpt/",
        "https://mrncstt.github.io/posts/bi_people_follow/",
        "https://mrncstt.github.io/posts/tips_buenos_aires/",
        "https://mrncstt.github.io/posts/make_over_monday_2022_w_35/",
    ],
)]

etl = ProfileETL()
etl.load(data, schema)
etl.explode_blog_posts()
etl.display(truncate=False)
etl.stop()

```
