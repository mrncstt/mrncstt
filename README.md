<!-- Heading -->
<h3 align="center"><img src = "https://raw.githubusercontent.com/MartinHeinz/MartinHeinz/master/wave.gif" width = 30px> Hi there!</h3>

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder.appName("ProfileApp").getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("education", StringType(), True),
    StructField("interests", ArrayType(StringType()), True),
    StructField("blog_posts", ArrayType(StringType()), True),
    StructField("digital_garden", StringType(), True),
    StructField("contact_info", StringType(), True),
    StructField("linkedin", StringType(), True),
    StructField("dev_to", StringType(), True),
    StructField("medium", StringType(), True),
    StructField("blog_links", ArrayType(StringType()), True)
])

data = [(
    "Mariana",
    "Data analytics enthusiast",
    "B.E. Production Engineering from the Federal University of Rio Grande do Norte (Brazil)",
    ["music", "HQ", "data literacy"],
    [
        "Automating LinkedIn post extraction using Selenium and BeautifulSoup",
        "Seeking insights from a recording using Google Cloud Speech-to-text, Google Colab and ChatGPT",
        "Enhance Your Business Intelligence Skills: Influencers to Follow for Tableau, Qlik, and Power BI Content",
        "How was my trip to Buenos Aires",
        "Make Over Monday - Top 10 Countries in Military Spending"
    ],
    "I try to write regular blog posts, most of which you will find on my personal website mrncstt.github.io",
    "You can reach me on social media.",
    "https://www.linkedin.com/in/mrncstt",
    "https://dev.to/mrncstt",
    "https://medium.com/@mrncstt",
    [
        "https://mrncstt.github.io/posts/automatinglinkedin_post_extraction/",
        "https://mrncstt.github.io/posts/seeking_insights_from_a_recording_using_google_cloud_speech_to_text_google_colab_and_chatgpt/",
        "https://mrncstt.github.io/posts/bi_people_follow/",
        "https://mrncstt.github.io/posts/tips_buenos_aires/",
        "https://mrncstt.github.io/posts/make_over_monday_2022_w_35/"
    ]
)]

df = spark.createDataFrame(data, schema=schema)

pandas_df = df.toPandas()

for index, row in pandas_df.iterrows():
    print(f"Hi there! I'm {row['name']}")
    print(f"💼 I'm a {row['occupation']};")
    print(f"📈 I have done graduation in {row['education']}.")
    print("Things about myself:")
    print(f"💬 Ask me about: {', '.join(row['interests'])};")
    print("\nRecent blog posts:")
    for post, link in zip(row['blog_posts'], row['blog_links']):
        print(f"  - {post} - {link}")
    print(f"\nMy Digital Garden 🌱")
    print(f"{row['digital_garden']}")
    print(f"\n{row['contact_info']}")
    print(f"LinkedIn: {row['linkedin']}")
    print(f"Dev.to: {row['dev_to']}")
    print(f"Medium: {row['medium']}")
    print("\n" + "="*40 + "\n")

spark.stop()

````


![](https://github-readme-streak-stats.herokuapp.com/?user=mrncstt&theme=dark)