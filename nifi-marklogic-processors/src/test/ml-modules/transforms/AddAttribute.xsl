<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:map="http://marklogic.com/xdmp/map">
  <xsl:param name="context" as="map:map"/>
  <xsl:param name="params" as="map:map"/>
  <xsl:template match="/*">
    <xsl:copy>
      <xsl:attribute name="{(map:get($params,'name'),'transformed')[1]}"
                     select="(map:get($params,'value'),'UNDEFINED')[1]"/>
      <xsl:copy-of select="@*"/>
      <xsl:copy-of select="node()"/>
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>
