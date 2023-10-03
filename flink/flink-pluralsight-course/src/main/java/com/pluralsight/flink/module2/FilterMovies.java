package com.pluralsight.flink.module2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple3;
import scala.util.parsing.combinator.testing.Str;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterMovies {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        // Convert each line of csv into a Object
        DataSet<Movie> movies = lines.map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
            @Override
            public Movie map(Tuple3<Long, String, String> csvLine) throws Exception {
                String movieName = csvLine.f1;
                String[] genres = csvLine.f2.split("\\|");
                return new Movie(movieName, new HashSet<String>(Arrays.asList(genres)));
            }
        });

        movies.print();

        //Filter movies where genre contains Drama
        DataSet<Movie> filteredMovies = movies.filter(new FilterFunction<Movie>() {
            @Override
            public boolean filter(Movie movie) throws Exception {
                return movie.getGenres().contains("Drama");
            }
        });

        filteredMovies.print();

        // Save the content to local disk
        filteredMovies.writeAsText("filtered-output");
        // TO execute the processing plan we need to execute it implicitly
        env.execute();

    }
}

class Movie{
    private String name;
    private Set<String> genres;

    public Movie(String name, Set<String> genres) {
        this.name = name;
        this.genres = genres;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "name='" + name + '\'' +
                ", genres=" + genres +
                '}';
    }


}
