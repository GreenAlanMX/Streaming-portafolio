// ============================================
// JSON CONVERSION SCRIPT
// Convert content.json to unified collection format
// ============================================

// This script converts the existing content.json structure
// to the unified collection format

// Example conversion for movies:
/*
Original movie structure:
{
  "content_id": "M001",
  "title": "Advanced World",
  "genre": ["Sci-Fi", "Horror", "Drama"],
  "duration_minutes": 179,
  "release_year": 2020,
  "rating": 3.5,
  "views_count": 66721,
  "production_budget": 220088717
}

Converted to unified format:
{
  "content_id": "M001",
  "title": "Advanced World",
  "genre": ["Sci-Fi", "Horror", "Drama"],
  "content_type": "movie",
  "duration_minutes": 179,
  "release_year": 2020,
  "rating": 3.5,
  "views_count": 66721,
  "production_budget": NumberLong(220088717)
}
*/

// Example conversion for series:
/*
Original series structure:
{
  "content_id": "S001",
  "title": "The Investigators",
  "genre": ["Crime"],
  "seasons": 7,
  "episodes_per_season": [8, 14, 7, 17, 12, 13, 18],
  "avg_episode_duration": 50,
  "rating": 1.3,
  "total_views": 50315,
  "production_budget": 44041273
}

Converted to unified format:
{
  "content_id": "S001",
  "title": "The Investigators",
  "genre": ["Crime"],
  "content_type": "series",
  "seasons": 7,
  "episodes_per_season": [8, 14, 7, 17, 12, 13, 18],
  "avg_episode_duration": 50,
  "total_episodes": 89,  // Calculated from episodes_per_season
  "total_runtime_minutes": 4450,  // Calculated: total_episodes * avg_episode_duration
  "rating": 1.3,
  "views_count": 50315,  // Renamed from total_views
  "production_budget": NumberLong(44041273)
}
*/

// Node.js script to convert the JSON file
const fs = require('fs');

// Read the original content.json
const contentData = JSON.parse(fs.readFileSync('../data/content.json', 'utf8'));

// Convert movies
const convertedMovies = contentData.movies.map(movie => ({
  ...movie,
  content_type: "movie",
  production_budget: movie.production_budget // Keep as number, MongoDB will handle conversion
}));

// Convert series
const convertedSeries = contentData.series.map(series => {
  // Calculate total episodes
  const totalEpisodes = series.episodes_per_season.reduce((sum, episodes) => sum + episodes, 0);
  
  return {
    content_id: series.content_id,
    title: series.title,
    genre: series.genre,
    content_type: "series",
    seasons: series.seasons,
    episodes_per_season: series.episodes_per_season,
    avg_episode_duration: series.avg_episode_duration,
    total_episodes: totalEpisodes,
    total_runtime_minutes: totalEpisodes * series.avg_episode_duration,
    rating: series.rating,
    views_count: series.total_views, // Rename field
    production_budget: series.production_budget
  };
});

// Combine all content
const unifiedContent = [...convertedMovies, ...convertedSeries];

// Write to new file
fs.writeFileSync('../data/content_unified.json', JSON.stringify(unifiedContent, null, 2));

console.log(`Converted ${convertedMovies.length} movies and ${convertedSeries.length} series to unified format`);
console.log(`Total content items: ${unifiedContent.length}`);
console.log('Unified content saved to: ../data/content_unified.json');

// Also create a MongoDB import script
const mongoImportScript = `
// MongoDB import script for unified content
use movieAnalyticsDB

// Clear existing content
db.content.deleteMany({})

// Import unified content
// Run this command in terminal:
// mongoimport --db movieAnalyticsDB --collection content --file content_unified.json --jsonArray

print("Import script ready. Run the mongoimport command above to load the data.")
`;

fs.writeFileSync('../scripts/import_unified_content.js', mongoImportScript);

console.log('MongoDB import script created: ../scripts/import_unified_content.js');
