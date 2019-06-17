package ru.pavel2107.otus.hw14.rdbms.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.List;

@Entity
@Table( name ="genre")
@NoArgsConstructor
public @ToString class Genre {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column( name="id", nullable = false)
    @Setter @Getter private Long id;

    @Column( name = "name")
    @Setter @Getter private String name;

    @OneToMany( mappedBy = "genre", fetch = FetchType.LAZY)
    @Setter @Getter private List<Book> books;
}
