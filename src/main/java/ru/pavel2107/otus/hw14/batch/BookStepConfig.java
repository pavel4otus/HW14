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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import ru.pavel2107.otus.hw14.mongoDB.domain.MongoBook;
import ru.pavel2107.otus.hw14.rdbms.domain.Book;
import ru.pavel2107.otus.hw14.rdbms.domain.Comment;


@Configuration
@EnableBatchProcessing
public class BookStepConfig {
    private final Logger logger = LoggerFactory.getLogger("Batch");

    final private String SELECT_SQL = "select * from books";

    public JobBuilderFactory jobBuilderFactory;
    public StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private MongoTemplate mongoTemplate;
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public BookStepConfig(
            JobBuilderFactory jobBuilderFactory,
            StepBuilderFactory stepBuilderFactory,
            DataSource dataSource,
            MongoTemplate mongoTemplate,
            NamedParameterJdbcTemplate jdbcTemplate){

        this.jobBuilderFactory  = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.mongoTemplate      = mongoTemplate;
        this.dataSource         = dataSource;
        this.jdbcTemplate       = jdbcTemplate;

    }

    JdbcCursorItemReader<Book> readerBook(){
        JdbcCursorItemReader<Book> reader = new JdbcCursorItemReader<>();
        reader.setDataSource( dataSource);
        reader.setSql( SELECT_SQL);
        reader.setRowMapper( new BookRowMapper( jdbcTemplate));

        return reader;
    }

    private static class BookRowMapper implements RowMapper<Book> {

        private NamedParameterJdbcTemplate jdbcTemplate;

        BookRowMapper( NamedParameterJdbcTemplate jdbcTemplate){
            this.jdbcTemplate = jdbcTemplate;
        }


        @Override
        public Book mapRow(ResultSet resultSet, int i) throws SQLException {
            final String SELECT_COMMENTS_SQL = "select * from comments where book_id=:book_id";

            Book book = new Book();
            book.setName( resultSet.getString( "name"));
            book.setId(   resultSet.getLong( "id"));

            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue( "book_id", book.getId());

            CommentRowMapper commentRowMapper = new CommentRowMapper();
            List<Comment> comments = jdbcTemplate.query(SELECT_COMMENTS_SQL, params, commentRowMapper);
            book.setComments( comments);

            return book;

        }
    }


    static class CommentRowMapper implements RowMapper<Comment>{

        @Override
        public Comment mapRow(ResultSet resultSet, int i) throws SQLException {
            Comment comment = new Comment();
            comment.setName( resultSet.getString( "name"));
            comment.setComment( resultSet.getString( "comment"));
            comment.setDateTime( resultSet.getTimestamp( "datetime").toLocalDateTime() );

            return comment;
        }
    }



    class MongoWriterBook implements ItemWriter<MongoBook>{

        @Override
        public void write(List<? extends MongoBook> list) throws Exception {
            for( int i = 0; i < list.size(); i++){
                MongoBook mongoBook = list.get( i);
                logger.info( "Записываем Book.id=" + mongoBook.getId());
                mongoTemplate.save( mongoBook);
            }
        }
    }

    public ItemWriter writerBook(){
        return new MongoWriterBook();
    }

    public ItemProcessor processorBook(){
        return ( ItemProcessor<Book, MongoBook>) Book ->{
            MongoBook mongoBook = new MongoBook();
            mongoBook.setId(   Book.getId().toString());
            mongoBook.setName( Book.getName());
            return mongoBook;
        };
    }

    public Step stepBook(){
        TaskletStep stepBook = stepBuilderFactory.get("stepBook")
                .chunk(5)
                .reader(readerBook())
                .writer( writerBook())
                .processor(processorBook())
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
        return stepBook;
    }

}
