package ru.pavel2107.otus.hw14.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import ru.pavel2107.otus.hw14.mongoDB.domain.MongoGenre;
import ru.pavel2107.otus.hw14.rdbms.domain.Genre;


@Configuration
@EnableBatchProcessing
public class GenreStepConfig {
    private final Logger logger = LoggerFactory.getLogger("Batch");

    final private String SELECT_SQL = "select * from genre";

    public JobBuilderFactory jobBuilderFactory;
    public StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private MongoTemplate mongoTemplate;


    @Autowired
    public GenreStepConfig(
            JobBuilderFactory jobBuilderFactory,
            StepBuilderFactory stepBuilderFactory,
            DataSource dataSource,
            MongoTemplate mongoTemplate){

        this.jobBuilderFactory  = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.mongoTemplate      = mongoTemplate;
        this.dataSource         = dataSource;
    }

    JdbcCursorItemReader<Genre> readerGenre(){
        JdbcCursorItemReader<Genre> reader = new JdbcCursorItemReader<>();
        reader.setDataSource( dataSource);
        reader.setSql( SELECT_SQL);
        reader.setRowMapper( new GenreRowMapper());

        return reader;
    }

    private static class GenreRowMapper implements RowMapper<Genre> {

        @Override
        public Genre mapRow(ResultSet resultSet, int i) throws SQLException {
            Genre genre = new Genre();
            genre.setName( resultSet.getString( "name"));
            genre.setId(   resultSet.getLong( "id"));
            return genre;
        }
    }

   class MongoWriterGenre implements ItemWriter<MongoGenre>{

       @Override
       public void write(List<? extends MongoGenre> list) throws Exception {
           for( int i = 0; i < list.size(); i++){
               MongoGenre mongoGenre = list.get( i);
               logger.info( "Записываем genre.id=" + mongoGenre.getId());
               mongoTemplate.save( mongoGenre);
           }
       }
   }

   public ItemWriter writerGenre(){
        return new MongoWriterGenre();
   }

    public ItemProcessor processorGenre(){
        return ( ItemProcessor<Genre, MongoGenre>) genre ->{
            MongoGenre mongoGenre = new MongoGenre();
            mongoGenre.setId(   genre.getId().toString());
            mongoGenre.setName( genre.getName());
            return mongoGenre;
        };
    }

    public Step stepGenre(){
        TaskletStep stepGenre = stepBuilderFactory.get("stepGenre")
                .chunk(5)
                .reader(readerGenre())
                .writer( writerGenre())
                .processor(processorGenre())
                .listener(new ItemReadListener() {
                    public void beforeRead() { logger.info("Начало чтения"); }
                    public void afterRead(Object o) { logger.info("Конец чтения"); }
                    public void onReadError(Exception e) { logger.info("Ошибка чтения"); }
                })
                .listener(new ItemWriteListener() {
                    public void beforeWrite(List list) { logger.info("Начало записи"); }
                    public void afterWrite(List list) { logger.info("Конец записи"); }
                    public void onWriteError(Exception e, List list) { logger.info("Ошибка записи"); }
                })
                .listener(new ItemProcessListener() {
                    public void beforeProcess(Object o) {logger.info("Начало обработки");}
                    public void afterProcess(Object o, Object o2) {logger.info("Конец обработки");}
                    public void onProcessError(Object o, Exception e) {logger.info("Ошбка обработки");}
                })
                .listener(new ChunkListener() {
                    public void beforeChunk(ChunkContext chunkContext) {logger.info("Начало пачки");}
                    public void afterChunk(ChunkContext chunkContext) {logger.info("Конец пачки");}
                    public void afterChunkError(ChunkContext chunkContext) {logger.info("Ошибка пачки");}
                })

                .build();
        return stepGenre;
    }

}
