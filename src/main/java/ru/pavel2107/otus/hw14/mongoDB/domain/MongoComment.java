package ru.pavel2107.otus.hw14.mongoDB.domain;


import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;


//
// id - id комментария
// dateTime - время когда оставили
// name - имя пользователя
// comment - сам сомментарий
//

public class MongoComment {

    @Setter @Getter private String name;
    @Setter @Getter private LocalDateTime dateTime;
    @Setter @Getter private String comment;
}
