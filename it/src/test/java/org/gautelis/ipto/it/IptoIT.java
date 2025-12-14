package org.gautelis.ipto.it;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(IptoSetupExtension.class)
public @interface IptoIT {
}
