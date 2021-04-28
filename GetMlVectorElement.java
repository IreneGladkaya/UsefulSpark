import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
 
UserDefinedFunction getX = functions.udf((Vector v) -> v.apply(0), DataTypes.DoubleType);
UserDefinedFunction getY = functions.udf((Vector v) -> v.apply(1), DataTypes.DoubleType);
