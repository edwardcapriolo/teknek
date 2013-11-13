package technique.util;

import java.util.HashMap;
import java.util.Map;

public class MapBuilder {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Map makeMap(Object... objects) {
    HashMap m = new HashMap();
    for (int i = 0; i < objects.length; i = i + 2) {
      m.put(objects[i], objects[i + 1]);
    }
    return m;
  }
}
