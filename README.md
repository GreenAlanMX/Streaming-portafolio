# Comprehensive Practice: Data Analysis and Visualization
## Data Engineering - 9th Semester

### Learning Objectives
Upon completing this practice, students will be able to:

- Apply fundamental concepts of visual information modeling
- Compare and contrast relational and NoSQL databases
- Manipulate and transform data in CSV and JSON formats
- Perform basic statistical analyses and communicate results effectively
- Create a complete documented project for their professional portfolio

### Project Description
**Central Theme: Video Streaming Platform Performance Analysis**

Students will work with a synthetic dataset that simulates data from a streaming platform, including user information, content, viewing sessions, and performance metrics.

### Provided Datasets

#### 1. users.csv
```csv
user_id,age,country,subscription_type,registration_date,total_watch_time_hours
U001,25,Mexico,Premium,2023-01-15,245.5
U002,34,Colombia,Basic,2023-02-20,156.2
U003,28,Argentina,Premium,2023-01-22,189.7
U004,45,Chile,Basic,2023-03-01,98.3
U005,32,Peru,Standard,2023-02-14,167.9
```

#### 2. content.json
```json
{
  "movies": [
    {
      "content_id": "M001",
      "title": "Data Adventures",
      "genre": ["Action", "Sci-Fi"],
      "duration_minutes": 120,
      "release_year": 2023,
      "rating": 4.2,
      "views_count": 15420,
      "production_budget": 50000000
    },
    {
      "content_id": "M002",
      "title": "Analytics Kingdom",
      "genre": ["Fantasy", "Adventure"],
      "duration_minutes": 98,
      "release_year": 2024,
      "rating": 4.5,
      "views_count": 23150,
      "production_budget": 35000000
    }
  ],
  "series": [
    {
      "content_id": "S001", 
      "title": "Analytics Chronicles",
      "genre": ["Drama", "Technology"],
      "seasons": 3,
      "episodes_per_season": [10, 12, 8],
      "avg_episode_duration": 45,
      "rating": 4.7,
      "total_views": 89650,
      "production_budget": 120000000
    },
    {
      "content_id": "S002",
      "title": "Data Detectives",
      "genre": ["Crime", "Mystery"],
      "seasons": 2,
      "episodes_per_season": [8, 10],
      "avg_episode_duration": 52,
      "rating": 4.3,
      "total_views": 67420,
      "production_budget": 85000000
    }
  ]
}
```

#### 3. viewing_sessions.csv
```csv
session_id,user_id,content_id,watch_date,watch_duration_minutes,completion_percentage,device_type,quality_level
S001,U001,M001,2024-03-15,118,98.3,Smart TV,4K
S002,U002,S001,2024-03-15,42,93.3,Mobile,HD
S003,U003,M002,2024-03-16,95,96.9,Tablet,HD
S004,U001,S002,2024-03-16,156,100.0,Smart TV,4K
S005,U004,M001,2024-03-17,85,70.8,Mobile,SD
```

## Phase 1: Database Design

### Activity 1.1: Relational Model
**Deliverables:**
- ER diagram of the relational database
- SQL scripts for table creation with proper constraints
- Complex SQL queries for analysis (minimum 5 with joins, subqueries, and aggregations)
- Normalization justification and optimization documentation

**Suggested Technologies:** PostgreSQL, MySQL, SQLite

**Required Queries Examples:**
- Top 5 most-watched content by country
- User retention analysis by subscription type
- Revenue analysis by content genre
- Seasonal viewing patterns
- Device preference correlation with completion rates

### Activity 1.2: NoSQL Model
**Deliverables:**
- Collection/document design for MongoDB
- Data insertion scripts with proper indexing
- Aggregation pipelines (minimum 3 stages each)
- Performance comparison between relational and NoSQL approaches

**Suggested Technologies:** MongoDB, CouchDB, Firebase

**Required Aggregations:**
- User engagement metrics by demographics
- Content performance analytics
- Geographic distribution analysis
- Time-series viewing trends

**Evaluation Criteria:**
- Correct normalization
- Query optimization and indexing strategies
- Technical justification for technology choice
- Clear schema documentation
- Performance benchmarking

## Phase 2: Statistical Analysis

### Activity 2.1: Descriptive Statistics
**Deliverables:**
- Jupyter notebook with comprehensive exploratory data analysis
- Central tendency and dispersion measures calculation
- Outlier detection and pattern identification
- Distribution analysis of key variables
- Data quality assessment report

**Key Metrics to Analyze:**
- User engagement distribution
- Content popularity trends
- Viewing completion rates
- Device usage patterns
- Geographic viewing preferences

### Activity 2.2: Inferential Analysis
**Deliverables:**
- Hypothesis testing on user behavior patterns
- Correlation analysis between variables
- User segmentation using clustering algorithms
- Basic predictive modeling for user retention
- Statistical significance testing and interpretation

**Required Analyses:**
- **Hypothesis Testing:** Premium vs Basic user engagement
- **Correlation Analysis:** Age, viewing time, and completion rates
- **Clustering:** User behavior segmentation (K-means, hierarchical)
- **Regression:** Predict user retention based on viewing patterns
- **Time Series:** Seasonal viewing trends analysis

**Suggested Tools:**
- **Python:** pandas, numpy, scipy, scikit-learn, matplotlib, seaborn
- **R:** dplyr, ggplot2, tidyr, caret
- **Jupyter Notebooks** or **R Markdown**

## Phase 3: Data Visualization

### Activity 3.1: Interactive Dashboards
**Deliverables:**
- Executive KPI dashboard with real-time metrics
- Interactive visualizations by user segments
- Temporal trend analysis with drill-down capabilities
- Geographic comparison dashboards
- Mobile-responsive design implementation

**Required Dashboard Components:**
- **Executive Summary:** Key metrics overview
- **User Analytics:** Demographics and behavior
- **Content Performance:** Popularity and engagement metrics
- **Financial Insights:** Revenue and cost analysis
- **Geographic Analysis:** Regional performance comparison

### Activity 3.2: Data Storytelling
**Deliverables:**
- Executive presentation (maximum 10 slides)
- Infographic summary of key insights
- Written recommendations

**Storytelling Requirements:**
- Clear narrative structure
- Data-driven insights
- Business impact assessment
- Future recommendations
- Visual design excellence

**Suggested Tools:**
- **Business Intelligence:** Tableau, Power BI, Looker Studio
- **Python:** Plotly, Streamlit, Dash, Bokeh
- **Web Technologies:** D3.js, Chart.js for advanced visualizations
- **Design Tools:** Figma, Adobe Creative Suite for infographics

## Phase 4: Data Transformation and Integration

### Activity 4.1: ETL Pipeline Development
**Deliverables:**
- Automated transformation pipeline CSV ↔ JSON
- Data cleaning and validation scripts
- Incremental loading process implementation
- Comprehensive ETL process documentation
- Error handling and logging mechanisms

**Pipeline Requirements:**
- **Extract:** Multiple data source integration
- **Transform:** Data cleaning, validation, and enrichment
- **Load:** Efficient data loading strategies
- **Monitor:** Pipeline monitoring and alerting
- **Scale:** Performance optimization for large datasets

**Technical Implementation:**
- Data validation rules
- Error handling strategies
- Logging and monitoring
- Performance optimization
- Automated scheduling

**Suggested Technologies:**
- **Workflow Management:** Apache Airflow, Luigi, Prefect
- **Data Processing:** Python (pandas, Apache Beam), Spark
- **Cloud Platforms:** AWS Glue, Google Cloud Dataflow, Azure Data Factory

## Portfolio Structure

### GitHub Repository (Suggested Structure):
```
streaming-analytics-project/
├── README.md
├── docs/
│   ├── database-design.md
│   ├── statistical-analysis.md
│   ├── visualization-guide.md
│   └── etl-documentation.md
├── data/
│   ├── raw/
│   ├── processed/
│   ├── schemas/
│   └── samples/
├── sql/
│   ├── relational/
│   │   ├── create_tables.sql
│   │   ├── insert_data.sql
│   │   └── analysis_queries.sql
│   └── nosql/
│       ├── mongodb_setup.js
│       └── aggregation_pipelines.js
├── notebooks/
│   ├── 01_exploratory_analysis.ipynb
│   ├── 02_statistical_testing.ipynb
│   └── 03_predictive_modeling.ipynb
├── dashboards/
│   ├── streamlit_app.py
│   ├── tableau_workbook.twbx
│   └── assets/
├── etl/
│   ├── pipelines/
│   ├── transformations/
│   └── monitoring/
├── presentation/
│   ├── executive_slides.pptx
│   ├── infographic.png
│   └── demo_video.mp4
├── tests/
└── requirements.txt
```

## Project Status

### Completed Activities
- **Activity 1.1:** Relational Model Implementation (PostgreSQL)
- **Activity 1.2:** NoSQL Model Implementation (MongoDB)

### In Progress
- **Phase 2:** Statistical Analysis
- **Phase 3:** Data Visualization
- **Phase 4:** ETL Pipeline Development

### Technologies Used
- **Database:** PostgreSQL, MongoDB
- **Languages:** SQL, JavaScript (MongoDB), Python
- **Tools:** Docker, Git, Jupyter Notebooks
- **Platforms:** GitHub, Docker Hub

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Git
- MongoDB Shell (mongosh)
- PostgreSQL client

### Quick Start
1. Clone the repository
2. Navigate to the desired activity branch
3. Follow the specific README instructions for each phase
4. Use Docker Compose to set up the required services

### Branch Structure
- `main` - Main project documentation
- `activity-1.1-clean` - Relational database implementation
- `no-sql` - NoSQL database implementation
- `fase2` - Statistical analysis phase
- `fase3` - Data visualization phase
- `fase4` - ETL pipeline development

## Contributing
This is an academic project for Data Engineering course. Each phase should be completed following the specified deliverables and evaluation criteria.

## License
Academic project - Educational use only
