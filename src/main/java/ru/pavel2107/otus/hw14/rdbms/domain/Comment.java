package ru.pavel2107.otus.hw14.rdbms.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.time.LocalDateTime;


//
// id - id комментария
// dateTime - время когда оставили
// name - имя пользователя
// comment - сам сомментарий
//

@Entity
@Table( name = "comments")
@NoArgsConstructor
public @ToString class Comment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column( name="id", nullable = false)
    @Setter @Getter private Long id;

    @Column( name = "name")
    @Setter @Getter private String name;

    @Column( name = "datetime")
    @Setter @Getter private LocalDateTime dateTime;

    @Column( name = "comment")
    @Setter @Getter private String comment;

    @ManyToOne
    @JoinColumn( name="book_id")
    @Setter @Getter private Book book;

}
