package ru.pavel2107.otus.hw14.mongoDB.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;

@Document( collection = "authors")
public @ToString
class MongoAuthor {
    @Setter @Getter @Id private String id;
    @Setter @Getter private String name;
    @Setter @Getter private LocalDate birthDate;
    @Setter @Getter private String    email;
    @Setter @Getter private String    phone;
    @Setter @Getter private String    address;
}
