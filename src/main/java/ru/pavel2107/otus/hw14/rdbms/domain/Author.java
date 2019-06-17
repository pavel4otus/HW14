package ru.pavel2107.otus.hw14.rdbms.domain;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.List;

@Entity
@Table(name = "authors")
@NoArgsConstructor
public @ToString class Author {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column( name="id", nullable = false)
    @Setter @Getter private Long id;

    @Column( name = "name")
    @Setter @Getter private String name;

    @Column( name = "birthdate")
    @Setter @Getter private LocalDate birthDate;

    @Column( name ="email")
    @Setter @Getter private String    email;

    @Column( name= "phone")
    @Setter @Getter private String    phone;

    @Column( name = "address")
    @Setter @Getter private String    address;

}
