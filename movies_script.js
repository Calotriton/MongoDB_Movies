use entrega

//1. Analyze the collection using find.
db.movies.find()

// 2. Count how many documents (movies) are loaded.
db.movies.countDocuments()

//3. Insert a movie.
var nueva_pelicula = { "title": "Ejercicio MongoDB", "year": 2024, "cast": ["Alejandro Lopez"], "genre": "Educacion" }
db.movies.insertOne(nueva_pelicula)

//4. Delete the movie inserted in the previous point (in point 3).
db.movies.deleteOne({ title: "Ejercicio MongoDB" })

//5. Count how many movies have actors (cast) named "and". These actor names are incorrect.
var query5 = { cast: "and" }
db.movies.find(query5).count()

//6. Update the documents whose actor (cast) has the incorrect value "and" as if it were a real actor. 
//This should only remove that value from the cast array. Therefore, neither the document (movie) nor its cast array should be deleted.
var query6 = {}
var operacion6 = { $pull: { "cast": "and" } }
db.movies.updateMany(query6, operacion6)

//7. Count how many documents (movies) have an empty 'cast' array.
var query7 = { cast: { $size: 0 } }
db.movies.find(query7).count()

//8. Update ALL documents (movies) that have an empty cast array by adding a new element within the array with the value "Undefined". Be careful! The cast type should still be an array. The array should look like -> ["Undefined"].
var query8 = { "cast": { $size: 0 } }
var filtro8 = { $set: { "cast": ["Undefined"] } }
db.movies.updateMany(query8, filtro8)
db.movies.find()

//9. Count how many documents (movies) have an empty genres array.
var query9 = { "genres": { $size: 0 } }
db.movies.find(query9).count()

//10. Update ALL documents (movies) that have an empty genres array by adding a new element within the array with the value "Undefined". Be careful! The genres type should still be an array. The array should look like -> ["Undefined"].
var query10 = { "genres": { $size: 0 } }
var filtro10 = { $set: { "genres": ["Undefined"] } }
db.movies.updateMany(query10, filtro10)
db.movies.find()

//11. Show the most recent/current year we have across all movies.
var query11 = {}
var operacion11 = { "_id": 0, "year": 1 }
db.movies.find(query11, operacion11).sort({ "year": -1 }).limit(1)

//12. Count how many movies were released in the last 20 years. It should be done from the latest year recorded in the collection, showing the total number for those years. This should be done using the Aggregation Framework.
var query12 = { "year": { $gte: 1998, $lte: 2018 } }
var operacion12 = [{ $match: query12 }, { $count: "total" }]
db.movies.aggregate(operacion12)

//13. Count how many movies were released in the 60s (from 1960 to 1969 inclusive). This should be done using the Aggregation Framework.
var query13 = { "year": { $gte: 1960, $lte: 1969 } }
var operacion13 = [{ $match: query13 }, { $count: "total" }]
db.movies.aggregate(operacion13)

//14. Show the year(s) with the most movies, showing the number of movies from that year. Check if multiple years can share the most number of movies.
var query14 = { _id: "$year", total: { $sum: 1 } }
var operacion14 = [{ $group: query14 }, { $sort: { total: -1 } }]
db.movies.aggregate(operacion14).limit(3)

//15. Show the year(s) with the fewest movies, showing the number of movies from that year. Check if multiple years can share the least number of movies.
var query15 = { _id: "$year", total: { $sum: 1 } }
var operacion15 = [{ $group: query15 }, { $sort: { total: 1 } }]
db.movies.aggregate(operacion15).limit(4)

//16. Save in a new collection called "actors" by performing the $unwind stage by actor. Then, count how many documents exist in the new collection.
var fase1 = { $unwind: "$cast" }
var query16 = { "_id": 0 }
var fase2 = { $project: query16 }
var fase3 = { $out: "actors" }
var etapas = [ fase1, fase2, fase3 ]
db.movies.aggregate( etapas )
db.actors.countDocuments()

//17. In the actors collection (new collection), show the list of the top 5 actors who have participated in the most movies, showing the number of movies they have participated in. 
//Important! First, filter out actors named "Undefined". Note that they are not deleted from the collection, they are just filtered out.
var query17 = {_id: "$cast", cuenta: { $sum: 1 }}
var operacion17 = [
    { $match: { cast: { $ne: "Undefined" } } },
    { $group: query17 },
    { $sort: { cuenta: -1 }}]
db.actors.aggregate(operacion17).limit(5)

//18. In the actors collection (new collection), group by movie and year, showing the top 5 movies with the most actors participating, displaying the total number of actors.
var query18 = {_id: { title: "$title", year: "$year" }, cuenta: { $sum: 1 }}
var operacion18 = [{ $group: query18 }, { $sort: { cuenta: -1 } }]
db.actors.aggregate(operacion18).limit(5)

// 19. In the actors collection (new collection), show the top 5 actors whose careers have lasted the longest. For this, you need to show when their career started, when it ended, and how many years they have worked. 
//Important! First, filter out actors named "Undefined". Note that they are not deleted from the collection, they are just filtered out.
var query19 = {_id: "$cast", comienza: { $min: "$year" }, termina: { $max: "$year" }}
var operacion19 = [
    { $match: { cast: { $ne: "Undefined" } } },
    { $group: query19 },
    { $project: { comienza: 1, termina: 1, anos: { $subtract: ["$termina", "$comienza"] } } },
    { $sort: { anos: -1 }}]
db.actors.aggregate(operacion19).limit(5)

// 20. In the actors collection (new collection), save in a new collection called “genres” by performing the $unwind stage by genres. Then, count how many documents exist in the new collection.
var fase1_2 = { $unwind: "$genres" }
var query20 = { "_id": 0}
var fase2_2 = { $project: query20 }
var fase3_2 = { $out: "genres" }
var etapas2 = [ fase1_2, fase2_2, fase3_2 ]
db.actors.aggregate( etapas2 )
db.genres.countDocuments()

// 21. In the genres collection (new collection), show the top 5 documents grouped by “Year and Genre” with the most different movies, showing the total number of movies.
var query21 = {_id: { year: "$year", genre: "$genres" }, pelis: { $sum: 1 }}
var operacion21 = [{ $group: query21 }, { $sort: { pelis: -1 } }]
db.genres.aggregate(operacion21).limit(5)

//22. In the genres collection (new collection), show the top 5 actors and the genres in which they have participated the most, showing the number of different genres they have acted in.
//Important! First, filter out actors named "Undefined". Note that they are not deleted from the collection, they are just filtered out.
var query22 = {_id: "$cast", generos: { $addToSet: "$genres" }};
var operacion22 = [
  { $match: { cast: { $ne: "Undefined" }}}, 
  { $group: query22 }, 
  { $project: { numgeneros: { $size: "$generos" }, generos: 1}},
  { $sort: { numgeneros: -1 } }]
db.genres.aggregate(operacion22).limit(5)

//23. In the genres collection (new collection), show the top 5 movies and their corresponding year in which the most different genres have been cataloged, showing those genres and the number of genres it contains.
var query23 = {_id: { title: "$title", year: "$year" }, generos: { $addToSet: "$genres" }}
var operacion23 = [
  { $match: { genres: { $ne: "Undefined" }}},  
  { $unwind: "$genres" },  
  { $group: query23 },  
  { $project: {_id: 1, generos: 1, numgeneros: { $size: "$generos" }}}, 
  { $sort: { numgeneros: -1 }}]

db.genres.aggregate(operacion23).limit(5)


//24. In the movies collection, find the 5 oldest movies that do not have an empty cast. Show its title, year and number of actors.
var query24 = { cast: { $exists: true, $not: { $size: 0}}}
var operacion24 = [
  { $match: query24 },
  { $project: { title: 1, year: 1, num_actores: { $size: "$cast" }}},
  { $sort: { year: 1 }}]
db.movies.aggregate(operacion24).limit(5)


//25. In the genres collection, find the 5 genres with the highest number of unique actors.
var query25 = { _id: "$genres", actores_unicos: { $addToSet: "$cast" }}
var operacion25 = [
  { $group: query25 },
  { $project: { genero: "$_id", num_actores_unicos: { $size: "$actores_unicos" }}},
  { $sort: { num_actores_unicos: -1 }}]
db.genres.aggregate(operacion25).limit(5)

//26. In the movies collection, show the 5 oldest movies that have more than one genre assigned. Show the title, year, and number of assigned genres.
var operacion26 = [
  { $match: query26 },
  { $project: { _id: 0, title: 1, year: 1, num_generos: { $size: "$genres" }}},
  { $sort: { year: 1 }}]
db.movies.aggregate(operacion26).limit(5)

