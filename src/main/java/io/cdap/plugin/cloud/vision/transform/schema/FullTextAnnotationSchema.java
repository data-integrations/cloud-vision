/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.cloud.vision.transform.schema;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * TextAnnotation contains a structured representation of OCR extracted text.
 * The hierarchy of an OCR extracted text structure is like this:
 * TextAnnotation -> Page -> Block -> Paragraph -> Word -> Symbol
 * Each structural component, starting from Page, may further have their own
 * properties.
 * {@link com.google.cloud.vision.v1.TextAnnotation} mapped to a record with following fields.
 */
public class FullTextAnnotationSchema {

  /**
   * UTF-8 text detected on the pages.
   */
  public static final String TEXT_FIELD_NAME = "text";
  /**
   * List of pages detected by OCR.
   */
  public static final String PAGES_FIELD_NAME = "pages";
  public static final Schema SCHEMA = Schema.recordOf(
          "fullTextAnnotation",
          Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(PAGES_FIELD_NAME, Schema.arrayOf(TextPage.SCHEMA)));

  private FullTextAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * A single symbol representation. {@link com.google.cloud.vision.v1.Symbol} mapped to a record with following fields.
   */
  public static class TextSymbol {

    /**
     * The actual UTF-8 representation of the symbol.
     */
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Confidence of the OCR results for the symbol. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * A list of detected languages together with confidence.
     */
    public static final String DETECTED_LANGUAGES_FIELD_NAME = "detectedLanguages";

    /**
     * Detected start or end of a text segment.
     */
    public static final String DETECTED_BREAK_FIELD_NAME = "detectedBreak";

    /**
     * The bounding box for the symbol. The vertices are in the order of top-left, top-right, bottom-right, bottom-left.
     * When a rotation of the bounding box is detected the rotation is represented as around the top-left corner as
     * defined when the text is read in the 'natural' orientation.
     * For example:
     * when the text is horizontal it might look like:
     * 0----1
     * |    |
     * 3----2
     * when it's rotated 180 degrees around the top-left corner it becomes:
     * 2----3
     * |    |
     * 1----0
     * and the vertex order will still be (0, 1, 2, 3).
     */
    public static final String BOUNDING_BOX_FIELD_NAME = "boundingBox";

    public static final Schema SCHEMA = Schema.recordOf(
            "document-text-page-symbol-record",
            Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(DETECTED_LANGUAGES_FIELD_NAME, Schema.nullableOf(Schema.arrayOf(
                    DetectedLanguage.getSchema("textSymbol-detectedLanguages")))),
            Schema.Field.of(DETECTED_BREAK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of(BOUNDING_BOX_FIELD_NAME, Schema.arrayOf(
                    VertexSchema.getSchema("textSymbol-boundingBox"))));
  }

  /**
   * A word representation. {@link com.google.cloud.vision.v1.Word} mapped to a record with following fields.
   */
  public static class TextWord {

    /**
     * The actual UTF-8 representation of the word.
     */
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Confidence of the OCR results for the word. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * List of symbols in the word. The order of the symbols follows the natural reading order.
     */
    public static final String SYMBOLS_FIELD_NAME = "symbols";

    /**
     * A list of detected languages together with confidence.
     */
    public static final String DETECTED_LANGUAGES_FIELD_NAME = "detectedLanguages";

    /**
     * Detected start or end of a text segment.
     */
    public static final String DETECTED_BREAK_FIELD_NAME = "detectedBreak";

    /**
     * The bounding box for the word. The vertices are in the order of top-left, top-right, bottom-right, bottom-left.
     * When a rotation of the bounding box is detected the rotation is represented as around the top-left corner as
     * defined when the text is read in the 'natural' orientation.
     * For example:
     * when the text is horizontal it might look like:
     * 0----1
     * |    |
     * 3----2
     * when it's rotated 180 degrees around the top-left corner it becomes:
     * 2----3
     * |    |
     * 1----0
     * and the vertice order will still be (0, 1, 2, 3).
     */
    public static final String BOUNDING_BOX_FIELD_NAME = "boundingBox";

    public static final Schema SCHEMA = Schema.recordOf(
            "document-text-page-word-record",
            Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(SYMBOLS_FIELD_NAME, Schema.arrayOf(TextSymbol.SCHEMA)),
            Schema.Field.of(DETECTED_LANGUAGES_FIELD_NAME, Schema.nullableOf(Schema.arrayOf(
                    DetectedLanguage.getSchema("textWord-detectedLanguage")))),
            Schema.Field.of(DETECTED_BREAK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of(BOUNDING_BOX_FIELD_NAME, Schema.arrayOf(
                    VertexSchema.getSchema("textWord-boundingBox"))));
  }

  /**
   * A paragraph representation. {@link com.google.cloud.vision.v1.Paragraph} mapped to a record with following fields.
   */
  public static class TextParagraph {

    /**
     * The actual UTF-8 representation of the paragraph.
     */
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Confidence of the OCR results for the paragraph. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * List of words in this paragraph.
     */
    public static final String WORDS_FIELD_NAME = "words";

    /**
     * A list of detected languages together with confidence.
     */
    public static final String DETECTED_LANGUAGES_FIELD_NAME = "detectedLanguages";

    /**
     * Detected start or end of a text segment.
     */
    public static final String DETECTED_BREAK_FIELD_NAME = "detectedBreak";

    /**
     * The bounding box for the paragraph. The vertices are in the order of top-left, top-right, bottom-right,
     * bottom-left. When a rotation of the bounding box is detected the rotation is represented as around the top-left
     * corner as defined when the text is read in the 'natural' orientation.
     * For example:
     * when the text is horizontal it might look like:
     * 0----1
     * |    |
     * 3----2
     * when it's rotated 180 degrees around the top-left corner it becomes:
     * 2----3
     * |    |
     * 1----0
     * and the vertice order will still be (0, 1, 2, 3).
     */
    public static final String BOUNDING_BOX_FIELD_NAME = "boundingBox";

    public static final Schema SCHEMA = Schema.recordOf(
            "document-text-page-paragraph-record",
            Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(WORDS_FIELD_NAME, Schema.arrayOf(TextWord.SCHEMA)),
            Schema.Field.of(DETECTED_LANGUAGES_FIELD_NAME, Schema.nullableOf(Schema.arrayOf(
                    DetectedLanguage.getSchema("textParagraph-detectedLanguages")))),
            Schema.Field.of(DETECTED_BREAK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of(BOUNDING_BOX_FIELD_NAME, Schema.arrayOf(
                    VertexSchema.getSchema("textParagraph-boundingBox"))));
  }

  /**
   * Logical element on the page. {@link com.google.cloud.vision.v1.Block} mapped to a record with following fields.
   */
  public static class TextBlock {

    /**
     * The actual UTF-8 representation of the block.
     */
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Detected block type (text, image etc) for this block.
     */
    public static final String BLOCK_TYPE_FIELD_NAME = "blockType";

    /**
     * Confidence of the OCR results of the block. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * List of paragraphs in this block (if this blocks is of type text).
     */
    public static final String PARAGRAPHS_FIELD_NAME = "paragraphs";

    /**
     * A list of detected languages together with confidence.
     */
    public static final String DETECTED_LANGUAGES_FIELD_NAME = "detectedLanguages";

    /**
     * Detected start or end of a text segment.
     */
    public static final String DETECTED_BREAK_FIELD_NAME = "detectedBreak";

    /**
     * The bounding box for the block. The vertices are in the order of top-left, top-right, bottom-right, bottom-left.
     * When a rotation of the bounding box is detected the rotation is represented as around the top-left corner as
     * defined when the text is read in the 'natural' orientation.
     * For example:
     * when the text is horizontal it might look like:
     * 0----1
     * |    |
     * 3----2
     * when it's rotated 180 degrees around the top-left corner it becomes:
     * 2----3
     * |    |
     * 1----0
     * and the vertice order will still be (0, 1, 2, 3).
     */
    public static final String BOUNDING_BOX_FIELD_NAME = "boundingBox";

    public static final Schema SCHEMA = Schema.recordOf(
            "document-text-page-block-record",
            Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(BLOCK_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(PARAGRAPHS_FIELD_NAME, Schema.arrayOf(TextParagraph.SCHEMA)),
            Schema.Field.of(DETECTED_LANGUAGES_FIELD_NAME, Schema.nullableOf(Schema.arrayOf(
                    DetectedLanguage.getSchema("textBlock-detectedLanguages")))),
            Schema.Field.of(DETECTED_BREAK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of(BOUNDING_BOX_FIELD_NAME, Schema.arrayOf(
                    VertexSchema.getSchema("textBlock-boundingBox"))));
  }

  /**
   * Detected page from OCR. {@link com.google.cloud.vision.v1.Page} mapped to a record with following fields.
   */
  public static class TextPage {

    /**
     * The actual UTF-8 representation of the page.
     */
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Page width. For PDFs the unit is points. For images (including TIFFs) the unit is pixels.
     */
    public static final String WIDTH_FIELD_NAME = "width";

    /**
     * Page height. For PDFs the unit is points. For images (including TIFFs) the unit is pixels.
     */
    public static final String HEIGHT_FIELD_NAME = "height";

    /**
     * Confidence of the OCR results on the page. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * List of blocks of text, images etc on this page.
     */
    public static final String BLOCKS_FIELD_NAME = "blocks";

    /**
     * Property section that contains detected languages.
     */
    public static final String PROPERTY_FIELD_NAME = "property";

    /**
     * A list of detected languages together with confidence.
     */
    public static final String DETECTED_LANGUAGES_FIELD_NAME = "detectedLanguages";

    /**
     * Detected start or end of a text segment.
     */
    public static final String DETECTED_BREAK_FIELD_NAME = "detectedBreak";

    public static final Schema SCHEMA = Schema.recordOf(
            "document-text-page-record",
            Schema.Field.of(TEXT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(WIDTH_FIELD_NAME, Schema.of(Schema.Type.INT)),         //
            Schema.Field.of(HEIGHT_FIELD_NAME, Schema.of(Schema.Type.INT)),        //
            Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(BLOCKS_FIELD_NAME, Schema.arrayOf(TextBlock.SCHEMA)),  //
            Schema.Field.of(PROPERTY_FIELD_NAME, Schema.nullableOf(Schema.arrayOf(
                    DetectedLanguage.getSchema("textPage-property")))),
            Schema.Field.of(DETECTED_BREAK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  /**
   * Detected page from OCR. {@link com.google.cloud.vision.v1.TextAnnotation.DetectedLanguage} mapped to a record with
   * following fields.
   */
  public static class DetectedLanguage {

    /**
     * The BCP-47 language code, such as "en-US" or "sr-Latin". For more information, see
     * http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
     */
    public static final String LANGUAGE_CODE_FIELD_NAME = "languageCode";

    /**
     * Confidence of detected language. Range [0, 1].
     */
    public static final String CONFIDENCE_FIELD_NAME = "confidence";

    /**
     * Utility method to create a {@link Schema} with a specific name. This is useful to create uniquely named schemas
     * that will be combined into a larger {@link Schema}.
     *
     * @param name {@link String} containing the name to give to the returned {@link Schema}.
     * @return a {@link Schema} with the given name.
     */
    public static Schema getSchema(String name) {
      return Schema.recordOf(
              name,
              Schema.Field.of(LANGUAGE_CODE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
              Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));
    }
  }
}
